FROM python:3.11-slim

WORKDIR /app


RUN pip install --no-cache-dir kafka-python requests

COPY consumer.py .

CMD ["python", "-u", "consumer.py"]