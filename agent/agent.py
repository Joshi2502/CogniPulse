import json
import time
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from ollama_client import ask_llm
from policy import validate_action

print("🤖 Agent starting...")

# -----------------------------
# Kafka Consumer (alerts)
# -----------------------------
consumer = KafkaConsumer(
    "cogni.alerts",
    bootstrap_servers="redpanda:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="agent-group",
    auto_offset_reset="latest",
)

# -----------------------------
# Kafka Producer (actions)
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers="redpanda:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------------
# Database Connection
# -----------------------------
conn = psycopg2.connect(
    host="postgres",
    dbname="cogni",
    user="cogni",
    password="cogni"
)

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
            latest_temp = result[0] if result else 0

            print("\n📊 Latest Temperature:")
            print(latest_temp)

            # -----------------------------
            # Minimal Prompt (fast + stable)
            # -----------------------------
            prompt = f"""
Temperature: {latest_temp}
Severity: {alert['severity']}

Return JSON:
{{
  "action_type": "SHUTDOWN | COOLING | LOG_ONLY",
  "confidence": 0.0,
  "reasoning": ""
}}
"""

            print("\n🧠 Sending to LLM...")

            # -----------------------------
            # Call LLM
            # -----------------------------
            try:
                llm_response = ask_llm(prompt)

                print("\n🔎 RAW LLM TEXT:")
                print(llm_response)

                cleaned_json = extract_json(llm_response)

                print("\n🤖 Extracted JSON:")
                print(cleaned_json)

                decision = json.loads(cleaned_json)

            except Exception as e:
                print("\n❌ LLM Processing Error:", e)
                decision = {
                    "action_type": "LOG_ONLY",
                    "confidence": 0.0,
                    "reasoning": "LLM failure"
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