import json
import psycopg2
from kafka import KafkaConsumer

conn = psycopg2.connect(
    host="postgres",
    dbname="cogni",
    user="cogni",
    password="cogni"
)

cursor = conn.cursor()

with open("schema.sql", "r") as f:
    cursor.execute(f.read())
    conn.commit()

consumer = KafkaConsumer(
    "cogni.events",
    "cogni.alerts",
    "cogni.actions",
    bootstrap_servers="redpanda:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="persistence-group"
)

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
        cursor.execute(
            "INSERT INTO alert_events (device_id, timestamp, alert_type, severity, reason) VALUES (%s,%s,%s,%s,%s)",
            (data["device_id"], data["timestamp"], data["alert_type"], data["severity"], data["reason"])
        )

    elif topic == "cogni.actions":
        cursor.execute(
            "INSERT INTO action_events (device_id, timestamp, action_type, decision_reason, confidence) VALUES (%s,%s,%s,%s,%s)",
            (data["device_id"], data["timestamp"], data["action_type"], data["decision_reason"], data["confidence"])
        )

        status = "CRITICAL" if data["action_type"] == "SHUTDOWN" else "WARNING"

        cursor.execute(
            "UPDATE latest_state SET status=%s WHERE device_id=%s",
            (status, data["device_id"])
        )

    conn.commit()
