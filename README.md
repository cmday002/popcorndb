# do this

start the kafka broker

```
docker-compose up -d
```

sql tables

```sql
CREATE TABLE IF NOT EXISTS public.movie_counts
(
    movie_title text COLLATE pg_catalog."default" NOT NULL,
    watchers bigint,
    CONSTRAINT movie_counts_pkey PRIMARY KEY (movie_title)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.movie_counts
    OWNER to admin;

CREATE TABLE IF NOT EXISTS public.movie_counts_time_series
(
    movie_title character varying COLLATE pg_catalog."default" NOT NULL,
    window_start timestamp without time zone NOT NULL,
    watchers bigint,
    CONSTRAINT movie_counts_time_series_pkey PRIMARY KEY (movie_title, window_start)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.movie_counts_time_series
    OWNER to admin;
```

shell into the kafka broker container

```
docker exec -it -w /opt/kafka/bin broker sh
```

in the shell, create a kafka topic

```
./kafka-topics.sh --create --topic movie-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

exit the container shell

```
exit
```

active the python env

```
.venv/Scripts/activate
```

start generating events

```
python movie_events.py
```

```
docker exec -it flink bash

pip install --no-cache-dir apache-flink==1.19.*
pip install google
pip install google-api-core

bin/flink run -py /opt/flink-jobs/flink.py
```
