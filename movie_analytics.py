from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import ReduceFunction
import json

env = StreamExecutionEnvironment.get_execution_environment()

# source = KafkaSource.builder() \
#     .set_bootstrap_servers("localhost:9092") \
#     .set_group_id("movie-consumer") \
#     .set_topics("movie-events") \
#     .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
#     .build()

# ds = env.from_source(source, SimpleStringSchema(), "Kafka Source")
# ds = ds.map(lambda e: json.loads(e))
# ds = ds.map(lambda e: {"movie_title": e["movie_title"], "count": 1})


# class CountReducer(ReduceFunction):
#     def reduce(self, a, b):
#         return {"movie_title": a["movie_title"], "count": a["count"] + b["count"]}


# ds = ds.key_by(lambda e: e["movie_title"]).reduce(CountReducer())
# ds.print()

# env.execute("Real-Time Movie Watch Analytics")
