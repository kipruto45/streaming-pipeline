import abc
import structlog
import os
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from config.settings import settings

log = structlog.get_logger()

class BaseProducer(abc.ABC):
    def __init__(self, topic: str, schema_file: str = None):
        self.topic = topic
        
        # Kafka configuration
        self.conf = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'enable.idempotence': True,
            'acks': 'all'
        }
        
        # Schema Registry setup
        self.serializer = None
        if schema_file:
            sr_conf = {'url': settings.SCHEMA_REGISTRY_URL.replace('8081', '8083')} # Using the corrected port
            sr_client = SchemaRegistryClient(sr_conf)
            
            with open(schema_file) as f:
                schema_str = f.read()
            
            self.serializer = AvroSerializer(sr_client, schema_str)
            
        self.producer = Producer(self.conf)
        self.log = log.bind(topic=topic)

    def delivery_report(self, err, msg):
        if err:
            self.log.error('Delivery failed', error=str(err))
        else:
            self.log.debug('Message delivered', partition=msg.partition(), offset=msg.offset())

    @abc.abstractmethod
    def generate_event(self):
        pass

    def produce(self, event: dict):
        try:
            value = event
            if self.serializer:
                # In a real producer, we need a MessageField.VALUE context
                from confluent_kafka.serialization import SerializationContext, MessageField
                ctx = SerializationContext(self.topic, MessageField.VALUE)
                value = self.serializer(event, ctx)
            else:
                value = str(event).encode('utf-8')

            self.producer.produce(
                self.topic,
                value=value,
                on_delivery=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            self.log.error("Production error", error=str(e))

    def flush(self):
        self.log.info("Flushing producer...")
        self.producer.flush()

    def run(self, rate_limit_ms: float = 0.01):
        self.log.info("Starting producer loop", use_avro=bool(self.serializer))
        try:
            while True:
                event = self.generate_event()
                if event:
                    self.produce(event)
                import time
                time.sleep(rate_limit_ms)
        except KeyboardInterrupt:
            self.log.info("Producer stopped")
        finally:
            self.flush()
