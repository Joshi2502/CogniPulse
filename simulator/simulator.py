import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="redpanda:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

device_id = "machine-01"
temperature = 92.0
mode = "NORMAL"

while True:
    if mode == "NORMAL":
        temperature += random.uniform(-0.5, 0.5)
    elif mode == "OVERHEAT":
        temperature += random.uniform(3, 6)
    elif mode == "SHUTDOWN":
        temperature -= random.uniform(4, 7)
        if temperature < 70:
            mode = "NORMAL"

    event = {
        "device_id": device_id,
        "timestamp": int(time.time()),
        "temperature": round(temperature, 2),
        "vibration": round(random.uniform(0.01, 0.03), 4)
    }

    producer.send("cogni.events", event)

    
    print("Produced event:", event)

    if temperature > 95:
        mode = "OVERHEAT"

    time.sleep(1)
