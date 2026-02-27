# CogniPulse

Real-time cognitive anomaly detection system using:

- Redpanda
- C++
- Ollama (LLM)
- Postgres
- FastAPI

## Run

docker compose up --build

After startup:

docker exec -it <ollama_container_id> ollama pull llama3

Access MCP API:
http://localhost:8000/docs
