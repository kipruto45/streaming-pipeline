import random
import time
from producers.base_producer import BaseProducer

class ClickstreamProducer(BaseProducer):
    def __init__(self):
        super().__init__(topic='raw.clickstream.events')
        self.pages = ['home', 'product_page', 'cart', 'checkout', 'search']
        self.actions = ['click', 'view', 'scroll', 'exit']

    def generate_event(self):
        return {
            'user_id': f'user_{random.randint(1, 5000)}',
            'page': random.choice(self.pages),
            'action': random.choice(self.actions),
            'timestamp': int(time.time() * 1000),
        }

if __name__ == "__main__":
    producer = ClickstreamProducer()
    producer.run(rate_limit_ms=0.05)
