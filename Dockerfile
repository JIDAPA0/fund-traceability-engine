FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md .env.example ./
COPY src ./src
COPY pipelines ./pipelines
COPY sql ./sql
COPY docs ./docs
COPY data ./data
COPY outputs ./outputs

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[orchestration]"

CMD ["python", "pipelines/run_refresh_all.py"]
