from confluent_kafka import Consumer
from config.settings import settings
import structlog

log = structlog.get_logger()

class AlertConsumer:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'alert-consumer-group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.conf)

    def run(self):
        self.consumer.subscribe(['alerts.anomalies'])
        log.info("Started Alert Consumer")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None: continue
                if msg.error():
                    log.error("Consumer error", error=str(msg.error()))
                    continue

                alert = msg.value().decode('utf-8')
                log.warn("!!! ANOMALY DETECTED !!!", alert=alert)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = AlertConsumer()
    consumer.run()
