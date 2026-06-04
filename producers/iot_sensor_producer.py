import random
import time
from producers.base_producer import BaseProducer
from config.settings import settings

class IOTSensorProducer(BaseProducer):
    def __init__(self):
        super().__init__(
            topic='raw.iot.sensor.events',
            schema_file='schemas/sensor_event.avsc'
        )
        self.sensors = ['temp', 'pressure', 'humidity', 'vibration']

    def generate_event(self):
        return {
            'sensor_id': f'sensor_{random.randint(1, 1000)}',
            'type': random.choice(self.sensors),
            'value': round(random.gauss(50, 10), 2),
            'timestamp': int(time.time() * 1000),
        }

if __name__ == "__main__":
    producer = IOTSensorProducer()
    # Use a slightly slower rate for the default run to be safer
    producer.run(rate_limit_ms=0.01)
