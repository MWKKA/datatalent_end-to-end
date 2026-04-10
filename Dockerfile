FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Inclure uniquement le code d’ingestion (évite un contexte Docker de plusieurs Go).
COPY scripts ./scripts

CMD ["python", "-m", "scripts.pipeline.run_ingestion_job"]
