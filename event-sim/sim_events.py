import json
import time
import random
from kafka import KafkaProducer
import pandas as pd

events = ['UFC Fight', 'NFL Game', 'Taylor Swift Concert']

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

user_count = 10000  # number of simulated users

while True:
    event = {
        "user_id": f"user_{random.randint(1, user_count)}",
        "movie_title": random.choice(events),
        "event_type": "watch",
        "timestamp": int(time.time() * 1000)
    }
    producer.send("movie-events", value=event)
    print(event)
    time.sleep(0.01)  # ~100 events per second
