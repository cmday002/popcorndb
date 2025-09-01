from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import ReduceFunction
from pyflink.table import StreamTableEnvironment

import json


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.enable_checkpointing(10 * 1000)

    # Kafka Source with proper deserializer
    source = KafkaSource.builder() \
        .set_bootstrap_servers("broker:29092") \
        .set_group_id("movie-consumer") \
        .set_topics("movie-events") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "Kafka Source")

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

    table_name = 'movie_counts'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            movie_title VARCHAR,
            watcher_count bigint
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/mydb',
            'table-name' = '{table_name}',
            'username' = 'admin',
            'password' = 'secret',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env = StreamTableEnvironment.create(env)
    t_env.execute_sql(sink_ddl)
    print('loading into postgres')

    t_env.execute_sql(
        f"""
            INSERT INTO {table_name}
            SELECT 'test', 1
        """
    ).wait()

    env.execute("Real-Time Movie Watch Analytics")


if __name__ == "__main__":
    main()
