from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import ReduceFunction
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Kafka Source with proper deserializer
    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_group_id("movie-consumer") \
        .set_topics("movie-events") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # Parse JSON
    ds = ds.map(lambda e: json.loads(e))

    # Map to {movie_title, count}
    ds = ds.map(lambda e: {"movie_title": e["movie_title"], "count": 1})

    # Reduce counts per movie
    class CountReducer(ReduceFunction):
        def reduce(self, a, b):
            return {"movie_title": a["movie_title"], "count": a["count"] + b["count"]}

    ds = ds.key_by(lambda e: e["movie_title"]).reduce(CountReducer())

    # Print real-time counts
    ds.print()

    env.execute("Real-Time Movie Watch Analytics")

if __name__ == "__main__":
    main()