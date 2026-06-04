import argparse
import time
from producers.iot_sensor_producer import IOTSensorProducer

def seed_data(count, rate):
    producer = IOTSensorProducer()
    print(f"Seeding {count} events at {rate} events/sec...")
    
    for i in range(count):
        event = producer.generate_event()
        producer.produce(event)
        if i % 100 == 0:
            print(f"Produced {i} events")
        time.sleep(1.0 / rate)
    
    producer.flush()
    print("Seeding complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--rate", type=int, default=100)
    args = parser.parse_args()
    
    seed_data(args.count, args.rate)
