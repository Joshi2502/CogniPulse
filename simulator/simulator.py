import json
import time
import random
from kafka import KafkaProducer

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers="redpanda:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("✅ Simulator connected to Redpanda.")
        break
    except Exception as e:
        print(f"⏳ Waiting for Redpanda... ({e})")
        time.sleep(3)

device_id = "machine-01"
temperature = 60.0
mode = "NORMAL"

while True:
    if mode == "NORMAL":
        temperature += random.uniform(-0.5, 1.5)
        temperature = max(temperature, 50.0)
        if temperature > 95:
            mode = "OVERHEAT"
            print("⚠️  Entering OVERHEAT mode")
    elif mode == "OVERHEAT":
        temperature += random.uniform(3, 6)
        if temperature > 110:
            mode = "SHUTDOWN"
            print("🛑 Entering SHUTDOWN mode")
    elif mode == "SHUTDOWN":
        temperature -= random.uniform(4, 7)
        if temperature < 60:
            mode = "NORMAL"
            print("✅ Recovered to NORMAL mode")

    event = {
        "device_id": device_id,
        "timestamp": int(time.time()),
        "temperature": round(temperature, 2),
        "vibration": round(random.uniform(0.01, 0.03), 4)
    }

    producer.send("cogni.events", event)

    print(f"[{mode}] Produced event: {event}")

    time.sleep(1)
