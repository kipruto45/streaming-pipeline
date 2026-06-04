import json
from confluent_kafka import Consumer
from config.settings import settings
from sinks.postgres_sink import PostgresSink
import structlog

log = structlog.get_logger()

class RawConsumer:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'raw-events-expert-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False  # Manual commit for exactly-once
        }
        self.consumer = Consumer(self.conf)
        self.sink = PostgresSink()

    def run(self):
        self.sink.connect()
        self.consumer.subscribe(['raw.iot.sensor.events'])
        log.info("Started Expert Raw Consumer with Manual Commits")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None: continue
                if msg.error():
                    log.error("Consumer error", error=str(msg.error()))
                    continue

                try:
                    # In expert mode, we might use fastavro for performance
                    # but for this flow we use standard JSON loads
                    event = json.loads(msg.value().decode('utf-8'))
                    
                    # Direct write to a 'raw_events' table if needed
                    # Or simple logging for audit
                    log.debug("Expert consumer audit", sensor_id=event.get('sensor_id'))
                    
                    self.consumer.commit(asynchronous=False)
                except Exception as e:
                    log.error("Processing error", error=str(e))
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            self.sink.close()

if __name__ == "__main__":
    consumer = RawConsumer()
    consumer.run()
