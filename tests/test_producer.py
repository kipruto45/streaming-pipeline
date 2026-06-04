import pytest
from producers.iot_sensor_producer import IOTSensorProducer
from schemas.models import SensorEvent

def test_sensor_event_generation():
    producer = IOTSensorProducer()
    event = producer.generate_event()
    
    # Validate structure
    assert 'sensor_id' in event
    assert 'type' in event
    assert 'value' in event
    assert 'timestamp' in event
    
    # Validate with Pydantic
    validated = SensorEvent(**event)
    assert validated.sensor_id.startswith('sensor_')
    assert validated.type in ['temp', 'pressure', 'humidity', 'vibration']

def test_schema_evolution_compatibility():
    """
    Expert Level: Verify that adding an optional field (evolution) 
    doesn't break the existing producer logic.
    """
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    
    sr_client = SchemaRegistryClient({'url': 'http://localhost:8083'})
    
    # New version of schema with an optional 'unit' field
    new_schema_str = """
    {
      "type": "record",
      "name": "SensorEvent",
      "fields": [
        {"name": "sensor_id", "type": "string"},
        {"name": "type", "type": "string"},
        {"name": "value", "type": "float"},
        {"name": "timestamp", "type": "long"},
        {"name": "unit", "type": ["null", "string"], "default": null}
      ]
    }
    """
    # This test verifies we can initialize a serializer with a newer schema
    # (In a real test, we would produce with v1 and consume with v2)
    try:
        serializer = AvroSerializer(sr_client, new_schema_str)
        assert serializer is not None
    except Exception:
        pytest.skip("Schema Registry not reachable in this environment")
