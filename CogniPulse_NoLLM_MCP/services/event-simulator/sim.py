import os, json, time, random
from datetime import datetime
from confluent_kafka import Producer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "cogni.events")
DEVICE_ID = os.getenv("DEVICE_ID", "factory-01")
RATE_HZ = float(os.getenv("RATE_HZ", "5"))
ANOMALY_PROB = float(os.getenv("ANOMALY_PROB", "0.05"))

producer = Producer({"bootstrap.servers": KAFKA_BROKERS})

def now_iso():
    return datetime.utcnow().isoformat() + "Z"

def sample():
    temperature = random.gauss(65, 3)
    vibration = max(0.0, random.gauss(3.5, 0.8))
    if random.random() < ANOMALY_PROB:
        temperature += random.uniform(15, 35)
    if random.random() < ANOMALY_PROB:
        vibration += random.uniform(4, 9)
    return {
        "event_id": f"evt-{time.time_ns()}",
        "device_id": DEVICE_ID,
        "temperature": round(temperature, 2),
        "vibration": round(vibration, 2),
        "ts": now_iso(),
    }

def main():
    period = 1.0 / max(RATE_HZ, 0.1)
    print(f"[sim] producing to {EVENTS_TOPIC} @ {RATE_HZ} Hz on {KAFKA_BROKERS}")
    while True:
        evt = sample()
        producer.produce(EVENTS_TOPIC, json.dumps(evt).encode("utf-8"))
        producer.poll(0)
        time.sleep(period)

if __name__ == "__main__":
    main()
