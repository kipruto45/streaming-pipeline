import os
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.functions import MapFunction

class MetadataEnricher(MapFunction):
    def __init__(self):
        # In real-world, this might load from a database or cache
        self.metadata = {
            "sensor_1": {"location": "Warehouse-A", "threshold": 80.0},
            "sensor_2": {"location": "Warehouse-B", "threshold": 95.0},
        }

    def map(self, value):
        sid = value[0]
        meta = self.metadata.get(sid, {"location": "Unknown", "threshold": 100.0})
        # Return enriched row: (sid, type, value, timestamp, location, threshold)
        return (value[0], value[1], value[2], value[3], meta["location"], meta["threshold"])

def run_enrichment():
    env = StreamExecutionEnvironment.get_execution_environment()
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')

    # Source
    deserializer = JsonRowDeserializationSchema.builder() \
        .type_info(Types.ROW_NAMED(["sensor_id", "type", "value", "timestamp"],
                                   [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.LONG()])).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics("raw.iot.sensor.events") \
        .set_group_id("flink-enrichment-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(deserializer).build()

    # Process
    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    enriched = ds.map(MetadataEnricher(), 
                      output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.LONG(), Types.STRING(), Types.DOUBLE()]))

    # Sink back to Kafka or a DB
    # For demo, we just print
    enriched.print()
    
    env.execute("Sensor Metadata Enrichment Job")

if __name__ == "__main__":
    run_enrichment()
