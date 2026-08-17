# keep-automation-consumer

Consumer service for the Keep **Automations** feature. It reads the
**matched-alerts topic**, runs the Redis gates (idempotency + cooldown), and calls
the automation API's idempotent submit endpoint — committing the matched-topic
offset only after an API-confirmed submit.

> **Status: C8 skeleton + C9 idempotency gate.** The consume loop runs the
> idempotency gate per message and logs the decision (duplicates are audited,
> everything else would submit). Offsets are still **never committed**
> (`enable.auto.commit=false`) and no submit call exists yet — those land in
> C10 (cooldown) and C11 (submit + offset commit + fail-open).

## Idempotency gate (C9)

`SET idem:{history_id}:{automation_id} "pending" NX EX 24h` per message
(`automation-contracts.md` §Redis keys, spec §5.2 step 1):

| Redis says | Outcome | Action |
|---|---|---|
| NX succeeded | `claimed` | submit |
| NX failed, value `done` | `duplicate` | audit `suppressed`(duplicate), commit offset, **no** submit |
| NX failed, value `pending` or key gone | `ambiguous` | **submit** |
| unreachable / `REDIS_URL` unset | `fail_open` | **submit**, `gate_flags={idempotency: skipped, reason: redis_unavailable}` |

The gate is an optimisation, never the authority: the API's unique
`(history_id, automation_id)` constraint is what actually dedups, and it
short-circuits to `200 already submitted` without calling `/run`. So every
ambiguous case submits — dropping a message would trade a cheap duplicate for a
lost automation. `done` is written only on an **API-confirmed** submit
(`mark_done`, wired in C11); there is no `release()` on failure, because
`pending` already routes a redelivery to a submit.

Redis being sick must cost ~0, not a timeout per message: socket timeouts bound
one message's latency, and a **circuit breaker** bounds the loop's throughput —
after `REDIS_BREAKER_FAILURE_THRESHOLD` consecutive failures the gate
short-circuits to fail-open without touching the socket, then lets one message
probe when the window expires. Redis-error tracebacks are budgeted (the metric
counts every failure; the log doesn't).

| Env var | Default | |
|---|---|---|
| `REDIS_URL` | *(empty)* | Empty = gates disabled, everything fails open |
| `IDEMPOTENCY_TTL_SECONDS` | `86400` | The contract value; floored at 1 (`EX 0` is a Redis error) |
| `REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS` / `REDIS_SOCKET_TIMEOUT_SECONDS` | `0.25` | The gate runs inside the poll loop |
| `REDIS_BREAKER_FAILURE_THRESHOLD` / `REDIS_BREAKER_OPEN_SECONDS` | `5` / `10.0` | Circuit breaker |
| `REDIS_ERROR_LOG_INTERVAL_SECONDS` | `30.0` | Traceback budget |

Metrics: `keep_automation_consumer_idempotency_decisions_total{outcome}`,
`keep_automation_consumer_redis_errors_total{operation}` (the Redis-down
signal), `keep_automation_consumer_gates_config_missing{setting}` (1 when
`REDIS_URL` is unset — "unconfigured" and "down" must be separable, and label
children are pre-initialised so an absent series only ever means "not
scraped").

Suppression audit rows go through `src/bl/suppression_audit.py` — a Protocol
with a logging stub until D17/D19 expose the endpoint.

## Layout (mirrors keep-event-handler)

| Path | Purpose |
|---|---|
| `src/consumer_main.py` | Standalone entrypoint: metrics server + health server + blocking consume loop. |
| `src/main.py` | FastAPI health/metrics app (K8s probes / scrape). |
| `src/core/kafka_consumer.py` | Consumer (auto-commit off) → subscribe → poll → deserialize → log. |
| `src/config/` | Env config + constants (topic, group, ports, worker-pool size, Redis). |
| `src/core/redis_client.py` | Process-lifetime Redis client (build once — a per-call pool leaks fds). |
| `src/models/matched_message.py` | Matched-message shape (contracts §"Matched message"). |
| `src/bl/gates/idempotency.py` | Idempotency gate (C9). Cooldown (C10) lands beside it. |
| `src/bl/suppression_audit.py` | Suppression audit Protocol + logging stub (D17 swaps it). |

Ports: health **8092**, metrics **8094**.

## Run locally

```bash
docker compose -f docker-compose.infra.yml up -d   # kafka + zookeeper
export REDIS_URL=redis://localhost:6379            # gates; unset = fail open
poetry install
poetry run python -m src.consumer_main             # start the consume loop
# in another shell, produce a test message to the matched-alerts topic
```

## Test

```bash
poetry run pytest
```
