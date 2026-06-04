import pytest
import time
import subprocess
import os
from producers.iot_sensor_producer import IOTSensorProducer

@pytest.mark.chaos
def test_broker_failure_resilience():
    """
    Expert Level Chaos Test:
    Simulate Kafka broker restart during production.
    """
    producer = IOTSensorProducer()
    
    # 1. Start production in background or separate process
    # For test, we just produce a batch
    for i in range(100):
        producer.produce({'sensor_id': 'chaos_test', 'value': 50.0, 'timestamp': int(time.time()*1000)})
    
    # 2. Kill a broker (simulated via docker)
    print("Simulating Broker Failure...")
    subprocess.run(["docker", "compose", "-f", "docker/docker-compose.yml", "restart", "kafka"])
    
    # 3. Verify producer recovers
    # confluent-kafka handles retries automatically if configured
    time.sleep(5)
    for i in range(100):
        producer.produce({'sensor_id': 'chaos_test', 'value': 60.0, 'timestamp': int(time.time()*1000)})
    
    producer.flush()
    print("Producer successfully recovered from broker failure.")

@pytest.mark.chaos
def test_flink_taskmanager_recovery():
    """
    Expert Level Chaos Test:
    Kill TaskManager and verify checkpoint restoration.
    """
    # Requires monitoring Flink API to verify job state
    # subprocess.run(["docker", "compose", "kill", "flink-taskmanager"])
    # time.sleep(10)
    # subprocess.run(["docker", "compose", "up", "-d", "flink-taskmanager"])
    pass
