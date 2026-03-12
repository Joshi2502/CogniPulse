import time
from fastapi import FastAPI
import psycopg2

app = FastAPI()

while True:
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="cogni",
            user="cogni",
            password="cogni"
        )
        print("✅ MCP server connected to Postgres.")
        break
    except Exception as e:
        print(f"⏳ Waiting for Postgres... ({e})")
        time.sleep(3)

@app.get("/devices/{device_id}")
def get_device(device_id: str):
    cur = conn.cursor()
    cur.execute("SELECT * FROM latest_state WHERE device_id=%s", (device_id,))
    return cur.fetchone()

@app.get("/alerts")
def get_alerts():
    cur = conn.cursor()
    cur.execute("SELECT * FROM alert_events ORDER BY timestamp DESC LIMIT 20")
    return cur.fetchall()

@app.get("/actions")
def get_actions():
    cur = conn.cursor()
    cur.execute("SELECT * FROM action_events ORDER BY timestamp DESC LIMIT 20")
    return cur.fetchall()

@app.get("/lineage")
def get_lineage():
    cur = conn.cursor()
    cur.execute("""
        SELECT
            act.id           AS action_id,
            act.action_type,
            act.decision_reason,
            act.confidence,
            act.timestamp    AS action_timestamp,

            al.id            AS alert_id,
            al.alert_type,
            al.severity,
            al.reason        AS alert_reason,
            al.timestamp     AS alert_timestamp,

            ev.id            AS event_id,
            ev.device_id,
            ev.temperature,
            ev.vibration,
            ev.timestamp     AS event_timestamp

        FROM action_events act
        JOIN alert_events  al ON act.alert_event_id     = al.id
        JOIN telemetry_events ev ON al.telemetry_event_id = ev.id
        ORDER BY act.timestamp DESC
        LIMIT 20
    """)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]
