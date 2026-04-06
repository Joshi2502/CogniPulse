# CogniPulse

**Real-time AI-powered industrial machine health monitoring.**

CogniPulse watches over industrial machines continuously, detects anomalies the moment they occur, and uses a local LLM to decide the right response — automatically, in seconds, with a plain-English explanation of every decision.

Built for the **Redpanda Hack the Planet Scholarship 2025**.

---

## What It Does

- Simulates 3 machines sending live sensor data (temperature, vibration, pressure, motor current)
- Detects anomalies in real time using a C++ threshold engine
- Uses **TinyLlama** (via Ollama, running locally) to decide and explain each action
- Persists every event, alert, and action to Postgres with full traceability
- Exposes a REST API to query device state, alert history, and action history

---

## Architecture

```
Event Simulator (Python)
        │
        │  cogni.telemetry
        ▼
  Redpanda Broker  ──────────────────────────────┐
  (Kafka Topics)                                  │ cogni.telemetry
  • cogni.telemetry                               ▼
  • cogni.alerts               ┌─────────────────────────┐
  • cogni.actions              │     C++ Analyzer         │
        │                      │  Anomaly Detection       │
        │ cogni.telemetry      │  Threshold Rules         │
        ▼                      └──────────┬──────────────┘
┌───────────────────┐                     │ cogni.alerts
│ Agent / Rule      │                     ▼
│ Engine (Python)   │         ┌─────────────────────────┐
│ • Alert eval      │────────▶│    Database Writer       │
│ • Action decisions│         │  • Consumes all topics   │
└───────────────────┘         │  • Stores historical data│
        │                     │  • Projects latest state │
        ▼                     └──────────┬──────────────┘
  Ollama / TinyLlama                     │
  (Local LLM)                            ▼
  Decides action +              ┌─────────────────┐
  generates reasoning           │   PostgreSQL     │
                                │ • event          │
                                │ • alert          │
                                │ • action         │
                                │ • latest_state   │
                                └────────┬────────┘
                                         │
                                         ▼
                                ┌─────────────────┐
                                │   MCP Server     │
                                │   (FastAPI)      │
                                │ • /devices/{id}  │
                                │ • /alerts        │
                                │ • /actions       │
                                │ • /lineage       │
                                └────────┬────────┘
                                         │
                                         ▼
                               Human / Agent Client
                               (CLI / UI Dashboard)
```

**Phase 2 (Future):** LLM Reasoning Layer — incident summaries, root cause analysis, and recommendations generated from historical Postgres data.

---

## Services

| Service | Language | Role |
|---|---|---|
| `simulator` | Python | Simulates 3 machines, emits telemetry to `cogni.telemetry` every second |
| `analyzer` | C++ (librdkafka) | Consumes telemetry, applies threshold rules, emits alerts to `cogni.alerts` |
| `agent` | Python | Consumes alerts, asks TinyLlama to decide + explain action, emits to `cogni.actions` |
| `persistence` | Python | Consumes all 3 topics, writes to Postgres |
| `mcp_server` | Python (FastAPI) | REST API exposing device state, alerts, actions, and lineage |
| `redpanda` | Docker image | Kafka-compatible message broker |
| `postgres` | Docker image | Persistent storage |
| `ollama` | Docker image | Local LLM inference server (TinyLlama) |

---

## Kafka Topics

| Topic | Payload |
|---|---|
| `cogni.telemetry` | `device_id`, `timestamp`, `metric_name`, `metric_value` |
| `cogni.alerts` | `device_id`, `timestamp`, `alert_type`, `severity`, `reason`, `metric_name`, `metric_value` |
| `cogni.actions` | `device_id`, `timestamp`, `action_type`, `decision_reason`, `confidence` |

---

## Anomaly Detection Rules (C++ Analyzer)

| Metric | Condition | Alert Type | Severity |
|---|---|---|---|
| Temperature | > 66°C | OVERHEAT | WARNING |
| Temperature | > 100°C | OVERHEAT | CRITICAL |
| Vibration | > 0.08 | HIGH_VIBRATION | WARNING |
| Vibration | > 0.15 | HIGH_VIBRATION | CRITICAL |
| Pressure | < 2.5 | LOW_PRESSURE | WARNING |
| Pressure | < 1.5 | LOW_PRESSURE | CRITICAL |

---

## Action Policy (AI Agent)

The agent asks TinyLlama to evaluate each alert and choose from:

| Action | Condition |
|---|---|
| `LOG_ONLY` | Temperature 66–70°C, low severity |
| `NOTIFY` | Temperature 70–78°C |
| `COOLING` | Temperature 78–100°C |
| `SHUTDOWN` | Temperature > 100°C or CRITICAL severity |

Every action is stored with a confidence score and a plain-English reasoning generated by TinyLlama.

---

## Database Schema

```
event ──< alert ──< action
  └──────────────── latest_state  (one row per device, upserted live)
```

---

## Getting Started

### Prerequisites
- Docker + Docker Compose

### Run

```bash
docker compose up --build
```

All 8 services start automatically. The system begins streaming, detecting, and acting within seconds.

### Pull the LLM model (first run only)

```bash
docker exec -it ollama ollama pull tinyllama
```


| Endpoint | Description |
|---|---|
| `GET /devices/{device_id}` | Latest state for a device |
| `GET /alerts` | Last 20 alerts |
| `GET /actions` | Last 20 actions |
| `GET /lineage` | Full event → alert → action chain |

---

## Simulated Machines

| Machine | Behaviour |
|---|---|
| `machine-01` | Stable — stays in LOG_ONLY temperature band |
| `machine-02` | Moderately hot — stays in NOTIFY band |
| `machine-03` | Degrades fast — cycles through COOLING → SHUTDOWN → recovery |

---

## Tech Stack

- **Python 3.11** — simulator, agent, persistence, MCP server
- **C++17** — analyzer (librdkafka + nlohmann/json)
- **Redpanda v23.1.2** — Kafka-compatible streaming broker
- **Ollama + TinyLlama** — local LLM inference, no internet required
- **PostgreSQL 15** — persistent storage
- **FastAPI** — REST API
- **Docker Compose** — orchestration

---

## Acknowledgements

Built during the **Redpanda Hack the Planet Scholarship 2025**.
