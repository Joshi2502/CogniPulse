import json
import time
import psycopg2
from kafka import KafkaConsumer

# Retry Postgres connection
while True:
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="cogni",
            user="cogni",
            password="cogni"
        )
        print("✅ Persistence connected to Postgres.")
        break
    except Exception as e:
        print(f"⏳ Waiting for Postgres... ({e})")
        time.sleep(3)

cursor = conn.cursor()

with open("schema.sql", "r") as f:
    for statement in f.read().split(";"):
        statement = statement.strip()
        if statement:
            cursor.execute(statement)
    conn.commit()

# Retry Redpanda connection
while True:
    try:
        consumer = KafkaConsumer(
            "cogni.events",
            "cogni.alerts",
            "cogni.actions",
            bootstrap_servers="redpanda:9092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="persistence-group"
        )
        print("✅ Persistence connected to Redpanda.")
        break
    except Exception as e:
        print(f"⏳ Waiting for Redpanda... ({e})")
        time.sleep(3)

for message in consumer:
    topic = message.topic
    data = message.value

    if topic == "cogni.events":
        # Each message is already one metric row
        cursor.execute(
            "INSERT INTO event (device_id, metric_name, metric_value, event_timestamp) VALUES (%s,%s,%s,%s)",
            (data["device_id"], data["metric_name"], data["metric_value"], data["timestamp"])
        )

        # Update latest_state when we receive temperature or vibration
        if data["metric_name"] == "temperature":
            cursor.execute("""
                INSERT INTO latest_state (device_id, last_seen, last_temperature, last_vibration, status)
                VALUES (%s,%s,%s,0,'NORMAL')
                ON CONFLICT (device_id)
                DO UPDATE SET last_seen=%s, last_temperature=%s
            """, (
                data["device_id"], data["timestamp"], data["metric_value"],
                data["timestamp"], data["metric_value"]
            ))
        elif data["metric_name"] == "vibration":
            cursor.execute("""
                INSERT INTO latest_state (device_id, last_seen, last_temperature, last_vibration, status)
                VALUES (%s,%s,0,%s,'NORMAL')
                ON CONFLICT (device_id)
                DO UPDATE SET last_seen=%s, last_vibration=%s
            """, (
                data["device_id"], data["timestamp"], data["metric_value"],
                data["timestamp"], data["metric_value"]
            ))

    elif topic == "cogni.alerts":
        # Link to the most recent temperature event for this device — retry until found
        event_id = None
        for _ in range(5):
            cursor.execute(
                "SELECT event_id FROM event WHERE device_id=%s AND metric_name='temperature' ORDER BY event_timestamp DESC LIMIT 1",
                (data["device_id"],)
            )
            row = cursor.fetchone()
            if row:
                event_id = row[0]
                break
            time.sleep(0.5)

        cursor.execute(
            "INSERT INTO alert (event_id, severity, alert_timestamp) VALUES (%s,%s,%s)",
            (event_id, data["severity"], data["timestamp"])
        )

    elif topic == "cogni.actions":
        # Link to the most recent alert for this device — retry until found
        alert_id = None
        for _ in range(5):
            cursor.execute(
                "SELECT alert_id FROM alert WHERE event_id IN (SELECT event_id FROM event WHERE device_id=%s) ORDER BY alert_timestamp DESC LIMIT 1",
                (data["device_id"],)
            )
            row = cursor.fetchone()
            if row:
                alert_id = row[0]
                break
            time.sleep(0.5)

        cursor.execute(
            "INSERT INTO action (alert_id, agent_id, action_taken, action_timestamp) VALUES (%s,%s,%s,%s)",
            (alert_id, "cogni-agent", data["action_type"], data["timestamp"])
        )

        status = "CRITICAL" if data["action_type"] == "SHUTDOWN" else "WARNING"
        cursor.execute(
            "UPDATE latest_state SET status=%s WHERE device_id=%s",
            (status, data["device_id"])
        )

    conn.commit()
