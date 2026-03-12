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
    cursor.execute(f.read())
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
        cursor.execute(
            "INSERT INTO telemetry_events (device_id, timestamp, temperature, vibration) VALUES (%s,%s,%s,%s)",
            (data["device_id"], data["timestamp"], data["temperature"], data["vibration"])
        )

        cursor.execute("""
            INSERT INTO latest_state (device_id, last_seen, last_temperature, last_vibration, status)
            VALUES (%s,%s,%s,%s,'NORMAL')
            ON CONFLICT (device_id)
            DO UPDATE SET last_seen=%s, last_temperature=%s, last_vibration=%s
        """, (
            data["device_id"], data["timestamp"], data["temperature"], data["vibration"],
            data["timestamp"], data["temperature"], data["vibration"]
        ))

    elif topic == "cogni.alerts":
        # Link to the most recent telemetry event for this device
        cursor.execute(
            "SELECT id FROM telemetry_events WHERE device_id=%s AND timestamp <= %s ORDER BY timestamp DESC LIMIT 1",
            (data["device_id"], data["timestamp"])
        )
        row = cursor.fetchone()
        telemetry_event_id = row[0] if row else None

        cursor.execute(
            "INSERT INTO alert_events (device_id, timestamp, alert_type, severity, reason, telemetry_event_id) VALUES (%s,%s,%s,%s,%s,%s)",
            (data["device_id"], data["timestamp"], data["alert_type"], data["severity"], data["reason"], telemetry_event_id)
        )

    elif topic == "cogni.actions":
        # Link to the most recent alert for this device
        cursor.execute(
            "SELECT id FROM alert_events WHERE device_id=%s AND timestamp <= %s ORDER BY timestamp DESC LIMIT 1",
            (data["device_id"], data["timestamp"])
        )
        row = cursor.fetchone()
        alert_event_id = row[0] if row else None

        cursor.execute(
            "INSERT INTO action_events (device_id, timestamp, action_type, decision_reason, confidence, alert_event_id) VALUES (%s,%s,%s,%s,%s,%s)",
            (data["device_id"], data["timestamp"], data["action_type"], data["decision_reason"], data["confidence"], alert_event_id)
        )

        status = "CRITICAL" if data["action_type"] == "SHUTDOWN" else "WARNING"

        cursor.execute(
            "UPDATE latest_state SET status=%s WHERE device_id=%s",
            (status, data["device_id"])
        )

    conn.commit()
