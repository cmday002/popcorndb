from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# ------------------------------
# Set up the Flink environment
# ------------------------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

t_env = StreamTableEnvironment.create(env)

# ------------------------------
# Define Kafka source table
# ------------------------------
t_env.execute_sql("""
CREATE TABLE movie_events (
    user_id STRING,
    movie_title STRING,
    event_type STRING,
    `timestamp` BIGINT,
    ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'movie-events',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink_movie_group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)
""")

# ------------------------------
# Define Postgres sink tables
# ------------------------------
t_env.execute_sql("""
CREATE TABLE movie_counts (
    movie_title STRING PRIMARY KEY NOT ENFORCED,
    watchers BIGINT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/mydb',
    'table-name' = 'movie_counts',
    'username' = 'admin',
    'password' = 'secret'
)
""")

t_env.execute_sql("""
CREATE TABLE movie_counts_time_series (
    movie_title STRING NOT NULL,
    window_start TIMESTAMP(3) NOT NULL,
    watchers BIGINT,
    PRIMARY KEY (movie_title, window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/mydb',
    'table-name' = 'movie_counts_time_series',
    'username' = 'admin',
    'password' = 'secret',
    'driver' = 'org.postgresql.Driver'
)
""")

# ------------------------------
# Define streaming aggregations
# ------------------------------
t_env.execute_sql("""
INSERT INTO movie_counts
SELECT
    movie_title,
    COUNT(user_id) AS watchers
FROM movie_events
GROUP BY movie_title
""")

t_env.execute_sql("""
INSERT INTO movie_counts_time_series
SELECT
    movie_title,
    TUMBLE_START(ts, INTERVAL '1' SECOND) AS window_start,
    COUNT(user_id) AS watchers
FROM movie_events
GROUP BY
    movie_title,
    TUMBLE(ts, INTERVAL '1' SECOND)
""")
