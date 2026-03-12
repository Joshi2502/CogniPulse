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

machines = {
    "machine-01": {"temperature": 60.0, "mode": "NORMAL"},
    "machine-02": {"temperature": 55.0, "mode": "NORMAL"},
    "machine-03": {"temperature": 65.0, "mode": "NORMAL"},
}

while True:
    for device_id, state in machines.items():
        mode = state["mode"]
        temperature = state["temperature"]

        if mode == "NORMAL":
            temperature += random.uniform(-0.5, 1.5)
            temperature = max(temperature, 50.0)
            if temperature > 95:
                state["mode"] = "OVERHEAT"
                print(f"⚠️  {device_id} entering OVERHEAT mode")
        elif mode == "OVERHEAT":
            temperature += random.uniform(3, 6)
            if temperature > 110:
                state["mode"] = "SHUTDOWN"
                print(f"🛑 {device_id} entering SHUTDOWN mode")
        elif mode == "SHUTDOWN":
            temperature -= random.uniform(4, 7)
            if temperature < 60:
                state["mode"] = "NORMAL"
                print(f"✅ {device_id} recovered to NORMAL mode")

        state["temperature"] = temperature

        event = {
            "device_id": device_id,
            "timestamp": int(time.time()),
            "temperature": round(temperature, 2),
            "vibration": round(random.uniform(0.01, 0.03), 4)
        }

        producer.send("cogni.events", event)
        print(f"[{device_id}][{state['mode']}] Produced event: {event}")

    time.sleep(1)
