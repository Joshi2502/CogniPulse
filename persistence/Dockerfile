FROM python:3.11-slim

WORKDIR /app

COPY db_writer.py .
COPY schema.sql .

RUN pip install kafka-python psycopg2-binary

CMD ["python", "db_writer.py"]
