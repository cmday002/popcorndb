import json
import time
import random
import math
from kafka import KafkaProducer
from multiprocessing import Process

# Kafka Producer config
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

user_count = 10000  # number of simulated users


def produce_events(event_name: str, traffic_pattern: str):
    start_time = time.time()

    while True:
        now = time.time() - start_time

        # Pick traffic behavior
        if traffic_pattern == "spiky":
            # Random short bursts: sometimes fast, sometimes slow
            sleep_time = random.choice([0.005, 0.01, 0.2])

        elif traffic_pattern == "noisy":
            # Around 50ms but with some noise
            sleep_time = max(0.01, random.normalvariate(0.05, 0.02))

        elif traffic_pattern == "parabolic":
            # Rate starts slow, speeds up, then slows again (parabola)
            peak_time = 60  # 1 minute to peak
            scale = abs((now - peak_time) / peak_time)
            sleep_time = 0.01 + scale * 0.2  # very fast near peak, slower at edges
        else:
            sleep_time = 0.05

        # Send event
        event = {
            "user_id": f"user_{random.randint(1, user_count)}",
            "movie_title": event_name,
            "event_type": "watch",
            "timestamp": int(time.time() * 1000)
        }
        producer.send("movie-events", value=event)
        print(f"[{event_name}] {event}")
        time.sleep(sleep_time)


if __name__ == "__main__":
    event_configs = [
        ("UFC Fight", "spiky"),
        ("NFL Game", "noisy"),
        ("Taylor Swift Concert", "parabolic"),
    ]

    processes = []
    for event_name, traffic_pattern in event_configs:
        p = Process(target=produce_events, args=(event_name, traffic_pattern))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
