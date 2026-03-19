import json
import time
import random
import math
from kafka import KafkaProducer

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers="redpanda:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("✅ Simulator connected to Redpanda.")
        break
    except Exception as e:
        print(f"⏳ Waiting for Redpanda... ({e})")
        time.sleep(3)

# Each machine has its own profile: baseline metrics, noise sensitivity, wear rate
machines = {
    "machine-01": {
        "mode": "NORMAL",
        "temperature": 58.0,
        "wear": 0.0,          # 0.0 = new, 1.0 = fully degraded
        "profile": {
            "base_temp": 58.0,
            "base_vibration": 0.015,
            "base_pressure": 4.2,    # bar
            "base_current": 12.0,    # amps
            "noise_factor": 1.0,
            "wear_rate": 0.0003,     # slow degradation
            "overheat_threshold": 95,
        }
    },
    "machine-02": {
        "mode": "NORMAL",
        "temperature": 54.0,
        "wear": 0.2,          # slightly worn already
        "profile": {
            "base_temp": 54.0,
            "base_vibration": 0.012,
            "base_pressure": 4.8,
            "base_current": 10.5,
            "noise_factor": 0.8,
            "wear_rate": 0.0001,     # very stable machine
            "overheat_threshold": 98,
        }
    },
    "machine-03": {
        "mode": "NORMAL",
        "temperature": 63.0,
        "wear": 0.5,          # already half-degraded — will fail more often
        "profile": {
            "base_temp": 63.0,
            "base_vibration": 0.025,
            "base_pressure": 3.9,
            "base_current": 14.0,
            "noise_factor": 1.4,
            "wear_rate": 0.0008,     # fast degradation
            "overheat_threshold": 90,
        }
    },
}


def gaussian(mean, std):
    return random.gauss(mean, std)


def spike(value, probability=0.02, magnitude=1.5):
    """Occasionally inject a sensor spike."""
    if random.random() < probability:
        return value * magnitude
    return value


def compute_metrics(device_id, state):
    """Derive all metrics from current machine state."""
    p = state["profile"]
    mode = state["mode"]
    wear = state["wear"]
    temp = state["temperature"]

    # Wear amplifies all deviations from baseline
    wear_multiplier = 1.0 + wear * 2.0

    # Vibration: correlated with temperature excess and wear
    temp_excess = max(0, temp - p["base_temp"])
    vibration = (
        p["base_vibration"]
        + temp_excess * 0.0004 * wear_multiplier
        + abs(gaussian(0, 0.002 * p["noise_factor"] * wear_multiplier))
    )
    vibration = spike(vibration)

    # Pressure: drops when machine overheats (coolant pressure loss)
    if mode == "NORMAL":
        pressure = p["base_pressure"] + gaussian(0, 0.05 * p["noise_factor"])
    elif mode == "OVERHEAT":
        pressure = p["base_pressure"] - 0.3 * wear_multiplier + gaussian(0, 0.1)
    else:  # SHUTDOWN
        pressure = p["base_pressure"] * 0.6 + gaussian(0, 0.05)
    pressure = spike(pressure, probability=0.01)

    # Motor current: rises with load (temperature proxy) and wear
    current = (
        p["base_current"]
        + temp_excess * 0.05 * wear_multiplier
        + gaussian(0, 0.3 * p["noise_factor"])
    )
    if mode == "SHUTDOWN":
        current = gaussian(0.5, 0.1)  # near zero when shut down
    current = max(0, spike(current, probability=0.015))

    return {
        "temperature": round(temp, 2),
        "vibration": round(max(0, vibration), 4),
        "pressure": round(max(0, pressure), 3),
        "motor_current": round(current, 2),
    }


def update_temperature(state):
    p = state["profile"]
    mode = state["mode"]
    wear = state["wear"]
    temp = state["temperature"]
    wear_multiplier = 1.0 + wear

    if mode == "NORMAL":
        # Gradual drift upward biased by wear (degraded machines run hotter)
        drift = gaussian(0.1 * wear_multiplier, 0.4 * p["noise_factor"])
        temp += drift
        temp = max(p["base_temp"] - 2, temp)
        if temp > p["overheat_threshold"]:
            state["mode"] = "OVERHEAT"
            print(f"⚠️  {state.get('_id', '')} entering OVERHEAT (wear={wear:.2f})")

    elif mode == "OVERHEAT":
        temp += gaussian(3.5 * wear_multiplier, 1.0)
        if temp > 110:
            state["mode"] = "SHUTDOWN"
            print(f"🛑 {state.get('_id', '')} entering SHUTDOWN")

    elif mode == "SHUTDOWN":
        temp -= gaussian(5.0, 1.0)
        if temp < p["base_temp"] + 2:
            state["mode"] = "NORMAL"
            print(f"✅ {state.get('_id', '')} recovered to NORMAL")

    state["temperature"] = temp


tick = 0

while True:
    tick += 1
    for device_id, state in machines.items():
        state["_id"] = device_id

        # Accumulate wear over time (only in NORMAL/OVERHEAT)
        if state["mode"] != "SHUTDOWN":
            state["wear"] = min(1.0, state["wear"] + state["profile"]["wear_rate"])

        update_temperature(state)
        metrics = compute_metrics(device_id, state)

        timestamp = int(time.time())

        # Emit one Kafka message per metric
        for metric_name, metric_value in metrics.items():
            event = {
                "device_id": device_id,
                "timestamp": timestamp,
                "metric_name": metric_name,
                "metric_value": metric_value,
            }
            producer.send("cogni.events", event)

        print(f"[{device_id}][{state['mode']}][wear={state['wear']:.2f}] "
              f"temp={metrics['temperature']} vib={metrics['vibration']} "
              f"pres={metrics['pressure']} cur={metrics['motor_current']}")

    time.sleep(1)
