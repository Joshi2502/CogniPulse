import os, json, time, random
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("BOOTSTRAP", "redpanda:9092")
OUT_TOPIC = os.getenv("OUT_TOPIC", "cogni.telemetry")
DEVICE_COUNT = int(os.getenv("DEVICE_COUNT", "5"))
INTERVAL_MS = int(os.getenv("INTERVAL_MS", "500"))

producer = Producer({"bootstrap.servers": BOOTSTRAP})

def now_epoch_s() -> float:
    return datetime.now(timezone.utc).timestamp()

def main():
    print(f"[event-simulator] BOOTSTRAP={BOOTSTRAP} topic={OUT_TOPIC} devices={DEVICE_COUNT}")
    while True:
        device_id = f"dev-{random.randint(1, DEVICE_COUNT)}"
        temp = round(random.uniform(55, 80) + (random.uniform(20, 40) if random.random() < 0.05 else 0), 2)
        vib = round(random.uniform(0.2, 1.5) + (random.uniform(1.5, 3.0) if random.random() < 0.03 else 0), 2)

        event = {"device_id": device_id, "temperature_c": temp, "vibration": vib, "ts": now_epoch_s()}

        producer.produce(OUT_TOPIC, key=device_id.encode(), value=json.dumps(event).encode())
        producer.poll(0)
        time.sleep(INTERVAL_MS / 1000.0)

if __name__ == "__main__":
    main()
