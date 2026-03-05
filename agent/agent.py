import json
import time
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from ollama_client import ask_llm
from policy import validate_action

print("🤖 Agent starting...")

# -----------------------------
# Kafka Consumer (alerts) — retry until Redpanda is ready
# -----------------------------
while True:
    try:
        consumer = KafkaConsumer(
            "cogni.alerts",
            bootstrap_servers="redpanda:9092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="agent-group",
            auto_offset_reset="latest",
        )
        producer = KafkaProducer(
            bootstrap_servers="redpanda:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("✅ Connected to Redpanda.")
        break
    except Exception as e:
        print(f"⏳ Waiting for Redpanda... ({e})")
        time.sleep(3)

# -----------------------------
# Database Connection — retry until Postgres is ready
# -----------------------------
while True:
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="cogni",
            user="cogni",
            password="cogni"
        )
        print("✅ Agent connected to Postgres.")
        break
    except Exception as e:
        print(f"⏳ Waiting for Postgres... ({e})")
        time.sleep(3)

print("✅ Agent connected to Kafka and Postgres.")
print("👂 Waiting for alerts...\n")

# -----------------------------
# Helper: Extract JSON safely
# -----------------------------
def extract_json(text: str) -> str:
    if not text:
        raise Exception("Empty LLM response")

    cleaned = text.strip()

    # Remove markdown fences
    if cleaned.startswith("```"):
        parts = cleaned.split("```")
        if len(parts) >= 2:
            cleaned = parts[1]
        cleaned = cleaned.replace("json", "").strip()

    # Extract JSON block
    start = cleaned.find("{")
    end = cleaned.rfind("}")

    if start == -1 or end == -1:
        raise Exception("No JSON object found in LLM response")

    return cleaned[start:end+1]


# -----------------------------
# Main Processing Loop
# -----------------------------
while True:
    for message in consumer:
        try:
            alert = message.value

            print("\n==============================")
            print("🚨 Received Alert:")
            print(alert)

            # -----------------------------
            # Fetch latest temperature ONLY
            # -----------------------------
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT temperature
                FROM telemetry_events
                WHERE device_id=%s
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (alert["device_id"],)
            )

            result = cursor.fetchone()
            latest_temp = result[0] if result else alert.get("temperature", 80.0)

            print("\n📊 Latest Temperature:")
            print(latest_temp)

            # -----------------------------
            # Deterministic action + confidence based on thresholds
            # -----------------------------
            severity = alert["severity"]

            if latest_temp > 100 or severity == "CRITICAL":
                action_type = "SHUTDOWN"
                confidence = round(min(0.7 + (latest_temp - 100) / 100, 1.0), 2) if latest_temp > 100 else 0.95
            elif latest_temp > 90:
                action_type = "COOLING"
                confidence = round(0.6 + (latest_temp - 90) / 100, 2)
            elif latest_temp > 75:
                action_type = "NOTIFY"
                confidence = round(0.5 + (latest_temp - 75) / 100, 2)
            else:
                action_type = "LOG_ONLY"
                confidence = round(0.5 + (latest_temp - 66) / 100, 2)

            print("\n🧠 Sending to LLM for reasoning...")

            # -----------------------------
            # LLM only generates the reasoning text
            # -----------------------------
            prompt = f"""A machine sensor reported: temperature {latest_temp}°C, severity {severity}.
The system has decided to take action: {action_type}.
In one sentence, explain why {action_type} is the appropriate response."""

            try:
                reasoning = ask_llm(prompt).strip().split("\n")[0]
            except Exception as e:
                print("\n❌ LLM Error:", e)
                reasoning = f"Temperature {latest_temp}°C with {severity} severity requires {action_type}."

            decision = {
                "action_type": action_type,
                "confidence": confidence,
                "reasoning": reasoning
            }

            # -----------------------------
            # Validate via policy
            # -----------------------------
            validated = validate_action(decision)

            print("\n✅ Validated Decision:")
            print(validated)

            # -----------------------------
            # Create action event
            # -----------------------------
            action_event = {
                "device_id": alert["device_id"],
                "timestamp": int(time.time()),
                "action_type": validated["action_type"],
                "decision_reason": validated["reasoning"],
                "confidence": float(validated["confidence"])
            }

            print("\n📤 Sending Action Event:")
            print(action_event)

            producer.send("cogni.actions", action_event)
            producer.flush()

            print("==============================\n")

        except Exception as fatal_error:
            print("\n🔥 AGENT LOOP ERROR:", fatal_error)

            try:
                conn.rollback()
            except:
                pass

            time.sleep(2)