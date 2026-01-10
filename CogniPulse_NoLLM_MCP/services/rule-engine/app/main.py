import os
import json
import time
import threading
from datetime import datetime
from typing import Any, Dict

from confluent_kafka import Consumer, Producer
from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import MongoClient

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "cogni.alerts")
ACTIONS_TOPIC = os.getenv("ACTIONS_TOPIC", "cogni.actions")
EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "cogni.events")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "cognipulse")

app = FastAPI(title="CogniPulse Rule Engine (No LLM)", version="0.1.0")

mongo = MongoClient(MONGO_URI)
db = mongo[MONGO_DB]
alerts_col = db["alerts"]
actions_col = db["actions"]

producer = Producer({"bootstrap.servers": KAFKA_BROKERS})

def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"

def decide(alert: Dict[str, Any]) -> Dict[str, Any]:
    # Deterministic policy (easy to upgrade later).
    severity = alert.get("severity", "low")
    metric = alert.get("metric", "")
    value = float(alert.get("value", 0) or 0)

    action = "notify"
    if severity in ("high", "critical"):
        if metric == "temperature":
            action = "activate_cooling_and_notify"
        elif metric == "vibration":
            action = "slow_down_and_notify"
        else:
            action = "notify_and_investigate"

    return {
        "action": action,
        "confidence": 0.75 if severity in ("high", "critical") else 0.55,
        "rationale": f"Rule policy: severity={severity}, metric={metric}, value={value} -> {action}",
        "ts": now_iso(),
    }

def publish_action(alert: Dict[str, Any], decision: Dict[str, Any]) -> None:
    payload = {
        "alert_id": alert.get("alert_id"),
        "device_id": alert.get("device_id"),
        "decision": decision,
        "alert": alert,
        "ts": now_iso(),
    }
    producer.produce(ACTIONS_TOPIC, json.dumps(payload).encode("utf-8"))
    producer.flush(5)

def alert_consumer_loop():
    c = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "cognipulse-rule-engine",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe([ALERTS_TOPIC])

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            time.sleep(0.2)
            continue
        try:
            alert = json.loads(msg.value().decode("utf-8"))
        except Exception:
            continue

        alerts_col.insert_one({"_id": alert.get("alert_id") or f"alert-{time.time_ns()}", **alert})
        decision = decide(alert)

        actions_col.insert_one({
            "_id": f"action-{time.time_ns()}",
            "decision": decision,
            "alert": alert,
            "ts": now_iso(),
        })

        publish_action(alert, decision)

@app.on_event("startup")
def _startup():
    t = threading.Thread(target=alert_consumer_loop, daemon=True)
    t.start()

class PublishEventRequest(BaseModel):
    device_id: str = "factory-01"
    temperature: float = 65.0
    vibration: float = 3.5

@app.get("/health")
def health():
    return {"ok": True, "ts": now_iso()}

@app.get("/latest/alerts")
def latest_alerts(limit: int = 20):
    docs = list(alerts_col.find().sort([("ts", -1)]).limit(limit))
    for d in docs:
        d["_id"] = str(d["_id"])
    return {"items": docs}

@app.get("/latest/actions")
def latest_actions(limit: int = 20):
    docs = list(actions_col.find().sort([("ts", -1)]).limit(limit))
    for d in docs:
        d["_id"] = str(d["_id"])
    return {"items": docs}

@app.post("/publish_test_event")
def publish_test_event(req: PublishEventRequest):
    evt = {
        "event_id": f"evt-{time.time_ns()}",
        "device_id": req.device_id,
        "temperature": req.temperature,
        "vibration": req.vibration,
        "ts": now_iso(),
    }
    producer.produce(EVENTS_TOPIC, json.dumps(evt).encode("utf-8"))
    producer.flush(5)
    return {"published": True, "event": evt}
