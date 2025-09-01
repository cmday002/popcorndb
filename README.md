# do this

start the kafka broker

```
docker-compose up -d
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
