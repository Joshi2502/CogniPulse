from fastapi import FastAPI
import psycopg2

app = FastAPI()

conn = psycopg2.connect(
    host="postgres",
    dbname="cogni",
    user="cogni",
    password="cogni"
)

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
