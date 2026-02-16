# CogniPulse

CogniPulse is an event-driven industrial telemetry pipeline with Redpanda, a C++ anomaly detector, a rule-based action engine, Postgres state storage, and MCP-backed read APIs.

## Architecture (Current)

1. **Event Generation** (`event-simulator`)
2. **Streaming Backbone** (Redpanda)
3. **Anomaly Detection** (`cpp-analyzer`)
4. **Decision Making** (`rule-engine`)
5. **Persistence + State** (Postgres via `mcp-server` consumer)
6. **MCP Interface** (`mcp-server` FastAPI reads)
7. **Human Client** (`visibility` Streamlit dashboard)
8. **LLM Reasoning Layer** (`llm-reasoner` + Ollama)

## Ollama Integration

This repo now includes a dedicated **LLM Reasoning Layer** that stays off the hot path:

- `ollama` service hosts local models.
- `ollama-pull` bootstraps the default model (`llama3.2:3b`).
- `llm-reasoner` fetches recent state/alerts/actions from MCP and asks Ollama for incident analysis.
- `visibility` can trigger LLM analysis for a selected device.

### Why this aligns with your architecture

- No operational actions are triggered by the LLM.
- LLM output is advisory only (incident summary, likely cause, recommended next steps).
- Deterministic rule engine continues to drive action publication.

## Run

```bash
docker compose up --build
```

## Main Endpoints

### MCP
- `GET /health`
- `GET /latest/device_state?device_id=dev-1`
- `GET /latest/alerts?limit=20`
- `GET /latest/actions?limit=20`

### LLM Reasoner
- `GET /health`
- `GET /reason/device?device_id=dev-1&limit=10`

The LLM reasoner response includes markdown in `analysis_markdown` for UI rendering.
