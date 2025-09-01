from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(
        env, environment_settings=settings)
    table_env.get_config().set_local_timezone('UTC')

    table_env.execute_sql("""
    create table movie_events_raw (
        user_id string,
        movie_title string,
        event_type string,
        timestamp timestamp(3),
        watermark for timestamp as timestamp - interval '10' second
    ) with (
        'connector' = 'kafka',
        'topic' = 'user-events',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'flink-user-group',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'  
    )
    """)

    table_env.execute_sql("""
    create table event_agg (
        movie_title string,
        current_watchers bigint
    ) with (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://mysql:5432/analytics',
        'table-name', 'event_agg',
        'driver' = 'com.mysql.jdbc.Driver',  
        'username' = 'demo',
        'password' = 'demo_password',
    )
    """)

    table_env.execute_sql("""
    insert into event_agg
    select movie_title
        , count(distinct user_id) current_watchers
    from movie_events_raw
    where event_type = 'started'
    """)


if __name__ == '__main__':
    main()
