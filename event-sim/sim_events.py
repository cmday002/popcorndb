import json
import time
import random
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
        elapsed = time.time() - start_time

        # --- UFC Fight ---
        if traffic_pattern == "ufc":
            # Start high ~30/s, then decline after 20s
            if elapsed < 20:
                rate = random.normalvariate(30, 5)  # avg 30/s
            else:
                # Decline linearly to ~5/s by 60s
                decline_factor = max(0, 1 - (elapsed - 20) / 40)
                rate = random.normalvariate(30 * decline_factor, 3)
            sleep_time = 1.0 / max(1, rate)

        # --- NFL Game ---
        elif traffic_pattern == "nfl":
            # Steady ~20/s, then spike to ~40/s at 30s
            if elapsed < 30:
                rate = random.normalvariate(20, 3)
            else:
                rate = random.normalvariate(40, 5)
            sleep_time = 1.0 / max(1, rate)

        # --- Taylor Swift Concert ---
        elif traffic_pattern == "taylor":
            # Linear growth from 10/s → 60/s over 60s
            growth = min(1.0, elapsed / 60.0)  # 0 → 1
            rate = random.normalvariate(10 + 50 * growth, 4)
            sleep_time = 1.0 / max(1, rate)

        else:
            sleep_time = 0.05  # fallback

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
        ("UFC Fight", "ufc"),
        ("NFL Game", "nfl"),
        ("Taylor Swift Concert", "taylor"),
    ]

    processes = []
    for event_name, traffic_pattern in event_configs:
        p = Process(target=produce_events, args=(event_name, traffic_pattern))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
