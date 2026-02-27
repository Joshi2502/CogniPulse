FROM python:3.11-slim
WORKDIR /app
COPY simulator.py .
RUN pip install kafka-python
CMD ["python", "simulator.py"]
