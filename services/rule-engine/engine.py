import os
import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.getenv("BOOTSTRAP", "redpanda:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "cogni.alerts")
OUT_TOPIC = os.getenv("OUT_TOPIC", "cogni.actions")
MCP_BASE_URL = os.getenv("MCP_BASE_URL", "http://mcp-server:8080")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "cognipulse-rule-engine",
    "auto.offset.reset": "latest",
})

producer = Producer({"bootstrap.servers": BOOTSTRAP})

def now_epoch_s() -> float:
    return datetime.now(timezone.utc).timestamp()

def decide(alert: dict) -> dict | None:
    device = alert.get("device_id", "unknown")
    severity = alert.get("severity", "unknown")
    reason = alert.get("reason", "unknown")

    if severity == "critical":
        return {
            "device_id": device,
            "action_type": "notify",
            "payload": {"channel": "ops", "priority": "p1"},
            "decision_reason": f"critical alert: {reason}",
            "action_ts": now_epoch_s(),
        }

    if severity == "warning":
        return {
            "device_id": device,
            "action_type": "notify",
            "payload": {"channel": "ops", "priority": "p2"},
            "decision_reason": f"warning alert: {reason}",
            "action_ts": now_epoch_s(),
        }

    return None

def main():
    print(f"[rule-engine] consume={IN_TOPIC} produce={OUT_TOPIC} mcp={MCP_BASE_URL}")

    consumer.subscribe([IN_TOPIC])

    # optional: quick MCP ping for debugging
    try:
        r = requests.get(f"{MCP_BASE_URL}/health", timeout=2)
        print(f"[rule-engine] mcp health: {r.status_code} {r.text}")
    except Exception as e:
        print(f"[rule-engine] mcp not reachable yet: {e}")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[rule-engine] consumer error: {msg.error()}")
            continue

        try:
            alert = json.loads(msg.value().decode("utf-8"))
        except Exception:
            print("[rule-engine] invalid json")
            continue

        action = decide(alert)
        if not action:
            continue

        producer.produce(
            OUT_TOPIC,
            key=action["device_id"].encode("utf-8"),
            value=json.dumps(action).encode("utf-8"),
        )
        producer.poll(0)

        print(f"[rule-engine] ACTION {action}")
        time.sleep(0.05)

if __name__ == "__main__":
    main()
