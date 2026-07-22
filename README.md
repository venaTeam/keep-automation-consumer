# keep-automation-consumer

Consumer service for the Keep **Automations** feature. It reads the
**matched-alerts topic**, runs the Redis gates (idempotency + cooldown), and calls
the automation API's idempotent submit endpoint — committing the matched-topic
offset only after an API-confirmed submit.

> **Status: C8 skeleton.** Currently only the service shell + a Kafka consume loop
> that **consumes and logs** matched messages. Offsets are **never committed**
> (`enable.auto.commit=false`) and there are no gates or submit calls yet —
> those land in C9 (idempotency), C10 (cooldown), C11 (submit + offset commit +
> fail-open).

## Layout (mirrors keep-event-handler)

| Path | Purpose |
|---|---|
| `src/consumer_main.py` | Standalone entrypoint: metrics server + health server + blocking consume loop. |
| `src/main.py` | FastAPI health/metrics app (K8s probes / scrape). |
| `src/core/kafka_consumer.py` | Consumer (auto-commit off) → subscribe → poll → deserialize → log. |
| `src/config/` | Env config + constants (topic, group, ports, worker-pool size). |
| `src/models/matched_message.py` | Matched-message shape (contracts §"Matched message"). |
| `src/bl/` | Placeholder for gates (C9/C10) + submit pipeline (C11). |

Ports: health **8092**, metrics **8094**.

## Run locally

```bash
docker compose -f docker-compose.infra.yml up -d   # kafka + zookeeper
poetry install
poetry run python -m src.consumer_main             # start the consume loop
# in another shell, produce a test message to the matched-alerts topic
```

## Test

```bash
poetry run pytest
```
