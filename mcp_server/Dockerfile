FROM python:3.11-slim

WORKDIR /app

COPY server.py .

RUN pip install fastapi uvicorn psycopg2-binary

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
