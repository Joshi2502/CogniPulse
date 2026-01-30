import os
import json
import threading
import time
from typing import Any, Dict, List

from fastapi import FastAPI, Query
import uvicorn

import psycopg2
from psycopg2.extras import RealDictCursor, Json

from kafka import KafkaConsumer


# ----------------------------
# App + config
# ----------------------------
app = FastAPI(title="CogniPulse MCP Server", version="0.1")

BOOTSTRAP = os.getenv("BOOTSTRAP", "redpanda:9092")
TELEMETRY_TOPIC = os.getenv("TELEMETRY_TOPIC", "cogni.telemetry")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "cogni.alerts")
ACTIONS_TOPIC = os.getenv("ACTIONS_TOPIC", "cogni.actions")

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "cognipulse")
PGUSER = os.getenv("PGUSER", "cognipulse")
PGPASSWORD = os.getenv("PGPASSWORD", "cognipulse")

# Stop flag for background thread
_stop_event = threading.Event()


def pg_conn():
    # short-lived connection per request is fine for MVP
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        cursor_factory=RealDictCursor,
    )


def _parse_json(value: Any) -> Dict[str, Any]:
    """
    KafkaConsumer gives bytes (then decoded) or already-structured strings.
    Normalize to dict.
    """
    if value is None:
        return {}
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="ignore")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {"raw": value}
    if isinstance(value, dict):
        return value
    return {"raw": str(value)}


def _persist_alert(payload: Dict[str, Any]) -> None:
    """
    alerts schema:
      device_id text not null
      severity text not null
      reason text not null
      source_ts timestamptz not null
      alert_ts timestamptz not null
      temperature_c double precision not null
      vibration double precision not null
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alerts(device_id, severity, reason, source_ts, alert_ts, temperature_c, vibration)
                VALUES (%s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s, %s)
                """,
                (
                    payload.get("device_id"),
                    payload.get("severity"),
                    payload.get("reason"),
                    float(payload.get("source_ts", time.time())),
                    float(payload.get("alert_ts", time.time())),
                    float(payload.get("temperature_c", 0.0)),
                    float(payload.get("vibration", 0.0)),
                ),
            )

            # Upsert device_state based on latest alert
            cur.execute(
                """
                INSERT INTO device_state(
                    device_id,
                    last_seen_ts,
                    severity,
                    last_reason,
                    temperature_c,
                    vibration,
                    last_alert_ts,
                    updated_at
                )
                VALUES (
                    %s,
                    to_timestamp(%s),
                    %s,
                    %s,
                    %s,
                    %s,
                    to_timestamp(%s),
                    now()
                )
                ON CONFLICT (device_id) DO UPDATE SET
                    last_seen_ts   = EXCLUDED.last_seen_ts,
                    severity       = EXCLUDED.severity,
                    last_reason    = EXCLUDED.last_reason,
                    temperature_c  = EXCLUDED.temperature_c,
                    vibration      = EXCLUDED.vibration,
                    last_alert_ts  = EXCLUDED.last_alert_ts,
                    updated_at     = now()
                """,
                (
                    payload.get("device_id"),
                    float(payload.get("source_ts", time.time())),
                    payload.get("severity"),
                    payload.get("reason"),
                    float(payload.get("temperature_c", 0.0)),
                    float(payload.get("vibration", 0.0)),
                    float(payload.get("alert_ts", time.time())),
                ),
            )


def _persist_action(payload: Dict[str, Any]) -> None:
    """
    actions schema:
      device_id text not null
      action_type text not null
      payload jsonb not null
      decision_reason text not null
      action_ts timestamptz not null
      status text not null default 'queued'
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO actions(device_id, action_type, payload, decision_reason, action_ts, status)
                VALUES (%s, %s, %s::jsonb, %s, to_timestamp(%s), %s)
                """,
                (
                    payload.get("device_id"),
                    payload.get("action_type"),
                    Json(payload.get("payload", {})),
                    payload.get("decision_reason", ""),
                    float(payload.get("action_ts", time.time())),
                    payload.get("status", "queued"),
                ),
            )

            # Update device_state with last_action_ts
            cur.execute(
                """
                INSERT INTO device_state(device_id, last_action_ts, updated_at)
                VALUES (%s, to_timestamp(%s), now())
                ON CONFLICT (device_id) DO UPDATE SET
                    last_action_ts = EXCLUDED.last_action_ts,
                    updated_at     = now()
                """,
                (
                    payload.get("device_id"),
                    float(payload.get("action_ts", time.time())),
                ),
            )


def consume_and_persist() -> None:
    """
    Background consumer for alerts + actions.
    """
    print(f"[mcp] consumer starting bootstrap={BOOTSTRAP} alerts={ALERTS_TOPIC} actions={ACTIONS_TOPIC}")

    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        ACTIONS_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="cognipulse-mcp",
        value_deserializer=lambda v: v.decode("utf-8", errors="ignore"),
    )

    try:
        while not _stop_event.is_set():
            for msg in consumer:
                if _stop_event.is_set():
                    break

                topic = msg.topic
                payload = _parse_json(msg.value)

                try:
                    if topic == ALERTS_TOPIC:
                        _persist_alert(payload)
                    elif topic == ACTIONS_TOPIC:
                        _persist_action(payload)
                except Exception as e:
                    # Don’t crash the thread; log and continue.
                    print(f"[mcp] persist error topic={topic} err={e} payload={payload}")

            time.sleep(0.2)
    finally:
        consumer.close()
        print("[mcp] consumer stopped")


# ----------------------------
# Health + basic endpoints
# ----------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "bootstrap": BOOTSTRAP,
        "topics": {
            "telemetry": TELEMETRY_TOPIC,
            "alerts": ALERTS_TOPIC,
            "actions": ACTIONS_TOPIC,
        },
        "postgres": {
            "host": PGHOST,
            "port": PGPORT,
            "db": PGDATABASE,
            "user": PGUSER,
        },
    }


# ----------------------------
# Postgres-backed reads
# ----------------------------
@app.get("/latest/device_state")
def get_latest_device_state(device_id: str = Query(..., description="Device ID like dev-1")):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT device_id, last_seen_ts, severity, last_reason,
                       temperature_c, vibration, last_alert_ts, last_action_ts, updated_at
                FROM device_state
                WHERE device_id = %s
                """,
                (device_id,),
            )
            row = cur.fetchone()
            return {"device_id": device_id, "state": row}


@app.get("/latest/alerts")
def get_latest_alerts(limit: int = 20):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, device_id, severity, reason, temperature_c, vibration, source_ts, alert_ts
                FROM alerts
                ORDER BY alert_ts DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            return {"count": len(rows), "alerts": rows}


@app.get("/latest/actions")
def get_latest_actions(limit: int = 20):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, device_id, action_type, payload, decision_reason, action_ts, status
                FROM actions
                ORDER BY action_ts DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            return {"count": len(rows), "actions": rows}


# ----------------------------
# Optional: topic debug tail
# ----------------------------
def read_last_messages(topic: str, n: int = 10) -> List[Dict[str, Any]]:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=1500,
        value_deserializer=lambda v: v.decode("utf-8", errors="ignore"),
    )

    msgs: List[Dict[str, Any]] = []
    try:
        for msg in consumer:
            msgs.append(_parse_json(msg.value))
            if len(msgs) >= n:
                break
    finally:
        consumer.close()

    return msgs


@app.get("/debug/topic_tail")
def topic_tail(topic: str, n: int = 10):
    return {"topic": topic, "messages": read_last_messages(topic, n)}


# ----------------------------
# Startup / shutdown hooks
# ----------------------------
@app.on_event("startup")
def _startup():
    # Start consumer thread only after app exists (fixes your NameError)
    t = threading.Thread(target=consume_and_persist, daemon=True)
    t.start()
    print("[mcp] background consumer thread started")


@app.on_event("shutdown")
def _shutdown():
    _stop_event.set()


# ----------------------------
# Main entrypoint
# ----------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
