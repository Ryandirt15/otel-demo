FROM python:3.11-slim

WORKDIR /app

COPY demo_three_pillars.py .

RUN pip install --no-cache-dir \
    opentelemetry-sdk \
    opentelemetry-exporter-otlp

CMD ["python", "demo_three_pillars.py"]
