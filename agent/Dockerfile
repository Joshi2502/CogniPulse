FROM python:3.11-slim

WORKDIR /app

COPY agent.py .
COPY ollama_client.py .
COPY policy.py .

RUN pip install kafka-python psycopg2-binary requests

CMD ["python", "agent.py"]
