from locust import User, task, between
import time
import random
import json
from confluent_kafka import Producer
from config.settings import settings

class KafkaUser(User):
    wait_time = between(0.001, 0.005) # Simulate high frequency

    def on_start(self):
        self.producer = Producer({'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS})
        self.topic = 'raw.iot.sensor.events'

    @task
    def produce_sensor_event(self):
        event = {
            'sensor_id': f'load_test_{random.randint(1, 10000)}',
            'type': random.choice(['temp', 'pressure', 'humidity']),
            'value': random.gauss(50, 10),
            'timestamp': int(time.time() * 1000)
        }
        self.producer.produce(self.topic, value=json.dumps(event).encode('utf-8'))
        self.producer.poll(0)

    def on_stop(self):
        self.producer.flush()
