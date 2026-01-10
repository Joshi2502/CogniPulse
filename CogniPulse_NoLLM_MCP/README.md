# CogniPulse (No LLM) + Redpanda MCP

End-to-end baseline framework:
- Redpanda topics: `cogni.events` -> `cogni.alerts` -> `cogni.actions`
- C++ analyzer (low-latency ingestion + anomaly detection)
- Rule engine (FastAPI) with deterministic decisions (NO LLM)
- Redpanda MCP server exposing CogniPulse tools via `rpk connect mcp-server`

## Run

```bash
docker compose down -v
docker compose up -d --build
docker compose logs -f cpp-analyzer


```

## Rule Engine API (no LLM)

- Health: `GET http://localhost:8000/health`
- Latest alerts: `GET http://localhost:8000/latest/alerts?limit=20`
- Latest actions: `GET http://localhost:8000/latest/actions?limit=20`
- Publish test event: `POST http://localhost:8000/publish_test_event`

Example:
```bash
curl -s http://localhost:8000/health
curl -s "http://localhost:8000/latest/alerts?limit=5" | python -m json.tool
curl -s "http://localhost:8000/latest/actions?limit=5" | python -m json.tool

curl -s -X POST http://localhost:8000/publish_test_event \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"factory-01","temperature":95,"vibration":2.0}' | python -m json.tool
```

## MCP Server

The MCP server is started by docker-compose on:

- `http://localhost:3333`

Tools are defined as Redpanda Connect resources in `services/mcp-server/resources/processors/`.
