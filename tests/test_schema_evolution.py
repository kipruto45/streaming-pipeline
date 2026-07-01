"""Schema evolution tests for Avro-based Kafka events.

Compatibility here means that producers and consumers may deploy independently,
so a consumer should continue to read data produced with an older or newer schema
without breaking the pipeline.
"""

import io
import json
from pathlib import Path

import fastavro
import pytest


SCHEMAS_DIR = Path(__file__).resolve().parents[1] / "schemas"
V1_SCHEMA_PATH = SCHEMAS_DIR / "iot_event_v1.avsc"
V2_SCHEMA_PATH = SCHEMAS_DIR / "iot_event_v2.avsc"


def _load_schema(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_v1_to_v2_deserialization_is_backward_compatible():
    v1_schema = _load_schema(V1_SCHEMA_PATH)
    v2_schema = _load_schema(V2_SCHEMA_PATH)

    event_v1 = {
        "device_id": "device-001",
        "timestamp": 1710000000000,
        "metric_name": "temperature",
        "value": 21.5,
        "unit": "C",
    }

    payload = io.BytesIO()
    fastavro.schemaless_writer(payload, v1_schema, event_v1)
    payload.seek(0)

    decoded = fastavro.schemaless_reader(payload, v1_schema, reader_schema=v2_schema)

    assert decoded["device_id"] == "device-001"
    assert decoded["metric_name"] == "temperature"
    assert decoded.get("firmware_version") is None
    assert decoded.get("signal_strength_dbm") is None


def test_v2_to_v1_deserialization_is_forward_compatible():
    v1_schema = _load_schema(V1_SCHEMA_PATH)
    v2_schema = _load_schema(V2_SCHEMA_PATH)

    event_v2 = {
        "device_id": "device-002",
        "timestamp": 1710000001000,
        "metric_name": "humidity",
        "value": 54.2,
        "unit": "%",
        "firmware_version": "v2.1.0",
        "signal_strength_dbm": -62.5,
    }

    payload = io.BytesIO()
    fastavro.schemaless_writer(payload, v2_schema, event_v2)
    payload.seek(0)

    decoded = fastavro.schemaless_reader(payload, v2_schema, reader_schema=v1_schema)

    assert decoded["device_id"] == "device-002"
    assert decoded["metric_name"] == "humidity"
    assert decoded["value"] == 54.2
    assert decoded["unit"] == "%"


def test_v2_schema_is_backward_compatible_with_defaults():
    v1_schema = _load_schema(V1_SCHEMA_PATH)
    v2_schema = _load_schema(V2_SCHEMA_PATH)

    v1_fields = {field["name"] for field in v1_schema["fields"]}
    v2_fields = {field["name"] for field in v2_schema["fields"]}
    new_fields = v2_fields - v1_fields

    assert new_fields, "Expected the v2 schema to add new fields"

    for field_name in new_fields:
        matching_field = next(field for field in v2_schema["fields"] if field["name"] == field_name)
        assert "default" in matching_field, f"Field {field_name} must define a default"
        assert matching_field["default"] is None, f"Field {field_name} should default to null"

    assert v2_schema["name"] == "IoTEvent"
    assert "firmware_version" in v2_fields
    assert "signal_strength_dbm" in v2_fields
