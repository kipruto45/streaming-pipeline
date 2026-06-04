import os
import json
import redis
from pyflink.common import WatermarkStrategy, Time, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction, RuntimeContext
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.datastream.functions import SinkFunction

# --- Redis Sink Implementation ---
class RedisSinkFunction(SinkFunction):
    def __init__(self, host='redis', port=6379):
        self.host = host
        self.port = port
        self.client = None

    def open(self, context: RuntimeContext):
        self.client = redis.Redis(host=self.host, port=self.port)

    def invoke(self, value, context):
        # value: (sid, start, end, avg, max, min, count)
        sid = value[0]
        data = {
            "avg": value[3],
            "max": value[4],
            "min": value[5],
            "count": value[6],
            "window_end": value[2]
        }
        self.client.hset("sensor:latest", sid, json.dumps(data))

    def close(self):
        if self.client:
            self.client.close()

# --- Aggregate Logic (Same as before but refined) ---
class SensorAggregate:
    def __init__(self, sid="", count=0, sum_val=0.0, max_val=-float('inf'), min_val=float('inf')):
        self.sid, self.count, self.sum_val, self.max_val, self.min_val = sid, count, sum_val, max_val, min_val

class SensorAggregateFunction(AggregateFunction):
    def create_accumulator(self): return SensorAggregate()
    def add(self, value, acc):
        acc.sid = value[0]
        acc.count += 1
        acc.sum_val += value[2]
        acc.max_val = max(acc.max_val, value[2])
        acc.min_val = min(acc.min_val, value[2])
        return acc
    def get_result(self, acc):
        avg = acc.sum_val / acc.count if acc.count > 0 else 0
        return (acc.sid, avg, acc.max_val, acc.min_val, acc.count)
    def merge(self, a, b):
        a.count += b.count
        a.sum_val += b.sum_val
        a.max_val = max(a.max_val, b.max_val)
        a.min_val = min(a.min_val, b.min_val)
        return a

class SensorWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        res = list(elements[0])
        res.insert(1, context.window().start)
        res.insert(2, context.window().end)
        yield tuple(res)

def run_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(60000)
    
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')

    deserializer = JsonRowDeserializationSchema.builder() \
        .type_info(Types.ROW_NAMED(["sensor_id", "type", "value", "timestamp"],
                                   [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.LONG()])).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics("raw.iot.sensor.events") \
        .set_group_id("flink-aggregator-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(deserializer).build()

    ds = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka Source")

    aggregated = ds.key_by(lambda r: r[0]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(60))) \
        .aggregate(SensorAggregateFunction(), window_function=SensorWindowFunction(),
                   output_type=Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.INT()]))

    # Sink 1: Postgres
    aggregated.add_sink(JdbcSink.sink(
        "INSERT INTO sensor_aggregates (sensor_id, window_start, window_end, avg_value, max_value, min_value, count) VALUES (?, TO_TIMESTAMP(? / 1000.0), TO_TIMESTAMP(? / 1000.0), ?, ?, ?, ?)",
        Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.INT()]),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder().with_url("jdbc:postgresql://postgres:5432/db").with_driver_name("org.postgresql.Driver").with_user_name("user").with_password("pw").build(),
        JdbcExecutionOptions.builder().with_batch_interval_ms(1000).with_batch_size(200).build()
    ))

    # Sink 2: Redis
    aggregated.add_sink(RedisSinkFunction())

    env.execute("Sensor Aggregator with Dual Sinks")

if __name__ == "__main__":
    run_aggregation()
