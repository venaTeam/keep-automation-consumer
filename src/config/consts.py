"""Consumer constants (spec §2.3, §5.2; topic/group names per A0 / contracts)."""
from src.config.config import config

# Kafka — matched-alerts topic between event-handler (matcher) and this consumer.
KAFKA_BOOTSTRAP_SERVERS = config("KAFKA_BOOTSTRAP_SERVERS", default="localhost:29092")
MATCHED_ALERTS_TOPIC = config("MATCHED_ALERTS_TOPIC", default="matched-alerts")
KAFKA_CONSUMER_GROUP = config("KAFKA_CONSUMER_GROUP", default="keep-automation-consumer")
KAFKA_AUTO_OFFSET_RESET = config("KAFKA_AUTO_OFFSET_RESET", default="earliest")
KAFKA_POLL_TIMEOUT_SECONDS = config("KAFKA_POLL_TIMEOUT_SECONDS", default="1.0", cast=float)

# Worker pool sized toward ~200 submits/s (spec §2.3). Config knob only in the
# skeleton — the pool itself arrives with submit orchestration (C11).
WORKER_POOL_SIZE = config("WORKER_POOL_SIZE", default="200", cast=int)

# Ports (event-handler convention: health 8092, metrics 8094).
HEALTH_CHECK_PORT = config("HEALTH_CHECK_PORT", default="8092", cast=int)
PROMETHEUS_METRICS_PORT = config("PROMETHEUS_METRICS_PORT", default="8094", cast=int)

# Redis — idempotency + cooldown gates (contracts §"Environment & endpoints").
# Empty is a supported state, not a misconfiguration to crash on: the gates are
# best-effort and fail open (spec §4.5, §6.3), so an unset URL degrades
# duplicate-protection and nothing else.
REDIS_URL = config("REDIS_URL", default="")

# Idempotency TTL. The contract value is **24h** (spec §7.6 / contracts §Redis
# keys) — it covers raw-topic redelivery + time-in-matched-topic + matched
# redelivery. Overridable per environment; the default is the contract, so an
# unset env is always correct. Lowering it only shrinks duplicate-suppression:
# the DB unique constraint on `(history_id, automation_id)` stays the dedup
# authority, so a short TTL costs extra idempotent submits, never correctness.
# Floored at 1 — `config()` does no validation, and `EX 0` is a Redis error
# ("invalid expire time") that would fail the claim on every message.
IDEMPOTENCY_TTL_SECONDS = max(
    1, config("IDEMPOTENCY_TTL_SECONDS", default="86400", cast=int)
)

# Hot-path bounds for every Redis call the gates make. The gate runs inside the
# Kafka poll loop, so an unreachable-but-not-refusing Redis must fail fast
# rather than push the loop toward `max.poll.interval.ms` (the rebalance-storm
# shape ADR-007 documents for keep-event-handler). Fail-open makes a timeout
# cheap: we submit without the gate.
REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS = config(
    "REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS", default="1.0", cast=float
)
REDIS_SOCKET_TIMEOUT_SECONDS = config(
    "REDIS_SOCKET_TIMEOUT_SECONDS", default="1.0", cast=float
)
