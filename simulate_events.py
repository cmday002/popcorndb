import json
import time
import random
from kafka import KafkaProducer

# Load your top 100 movies
movies = ["Inception", "The Dark Knight", "Interstellar", ...]  # list of 100

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

user_count = 10000  # number of simulated users

while True:
    event = {
        "user_id": f"user_{random.randint(1, user_count)}",
        "movie_title": random.choice(movies),
        "event_type": "watch",
        "timestamp": int(time.time())
    }
    producer.send("movie-events", value=event)
    time.sleep(0.01)  # ~100 events per second