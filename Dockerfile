FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir poetry

COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --without dev --no-root

COPY src/ ./src/
RUN poetry install --no-interaction --no-ansi --without dev

# Health 8092, metrics 8094 (event-handler convention)
EXPOSE 8092 8094

# Runs the standalone Kafka consume loop (starts its own health + metrics servers)
CMD ["python", "-m", "src.consumer_main"]
