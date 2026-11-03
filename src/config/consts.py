"""Consumer constants (spec §2.3, §5.2; topic/group names per A0 / contracts)."""
from src.config.config import config

# Kafka — matched-alerts topic between event-handler (matcher) and this consumer.
KAFKA_BOOTSTRAP_SERVERS = config("KAFKA_BOOTSTRAP_SERVERS", default="localhost:29092")
MATCHED_ALERTS_TOPIC = config("MATCHED_ALERTS_TOPIC", default="matched-alerts")
KAFKA_CONSUMER_GROUP = config("KAFKA_CONSUMER_GROUP", default="keep-automation-consumer")
KAFKA_AUTO_OFFSET_RESET = config("KAFKA_AUTO_OFFSET_RESET", default="earliest")
KAFKA_POLL_TIMEOUT_SECONDS = config("KAFKA_POLL_TIMEOUT_SECONDS", default="1.0", cast=float)
KAFKA_BACKPRESSURE_POLL_TIMEOUT_SECONDS = config(
    "KAFKA_BACKPRESSURE_POLL_TIMEOUT_SECONDS", default="0.1", cast=float
)

# How often settled offsets are committed (asynchronously, from the poll
# thread). A commit is a broker round trip; one per message at ~200/s would
# spend the poll thread on commits. Shutdown and revoke always commit
# synchronously regardless of this interval.
KAFKA_COMMIT_INTERVAL_SECONDS = max(
    0.0, config("KAFKA_COMMIT_INTERVAL_SECONDS", default="1.0", cast=float)
)

# Commits are contiguous per partition, so one message stuck retrying holds back
# every settled message behind it. Past this many uncommitted offsets the
# partition is paused (only that one) until the stuck message settles — bounds
# both redelivery after a crash and how far one automation can run ahead of it.
MAX_UNCOMMITTED_OFFSETS_PER_PARTITION = max(
    1, config("MAX_UNCOMMITTED_OFFSETS_PER_PARTITION", default="1000", cast=int)
)

# Worker pool sized toward ~200 short submit-acknowledgement round trips/s
# (spec §2.3, §5.2). Workers also hold messages that are backing off after a
# failed submit; when every slot is busy, partitions pause and polling goes on.
WORKER_POOL_SIZE = max(1, config("WORKER_POOL_SIZE", default="200", cast=int))

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

# Hot-path bounds for every Redis call the gates make. A timeout that fires is
# not protection, it is the cost: a blackholed Redis (SG drop, NLB draining,
# partition) charges the full connect timeout to EVERY message, so at the ~200
# msg/s target (spec §2.3) a 1s bound would drain the topic at ~1 msg/s. A
# healthy p99 is well under 1ms, so 0.25s is ~250x headroom and caps the
# blackhole case at ~4 msg/s per attempt — the breaker below carries the rest.
REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS = config(
    "REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS", default="0.25", cast=float
)
REDIS_SOCKET_TIMEOUT_SECONDS = config(
    "REDIS_SOCKET_TIMEOUT_SECONDS", default="0.25", cast=float
)

# Circuit breaker on the gate's Redis calls. Timeouts bound one message; they do
# not bound throughput — without this, a sick Redis costs a socket timeout on
# every message forever, and the gate is an optimisation, never the authority.
# An unavailable optimisation must cost ~0. After N consecutive failures the
# gate short-circuits to fail-open without touching the socket, then lets one
# message probe when the window expires (half-open).
REDIS_BREAKER_FAILURE_THRESHOLD = max(
    1, config("REDIS_BREAKER_FAILURE_THRESHOLD", default="5", cast=int)
)
REDIS_BREAKER_OPEN_SECONDS = max(
    1.0, config("REDIS_BREAKER_OPEN_SECONDS", default="10.0", cast=float)
)

# Traceback budget for the gate's Redis errors: a Redis outage at 200 msg/s
# would otherwise write 200 stack traces/s from the worker pool.
# The metric counts every failure; only the logging is budgeted.
REDIS_ERROR_LOG_INTERVAL_SECONDS = max(
    0.0, config("REDIS_ERROR_LOG_INTERVAL_SECONDS", default="30.0", cast=float)
)

# Automation API — `POST /internal/submit` (contracts §Submit, §Environment).
# Required: the consumer cannot do its job without it, so startup fails fast.
AUTOMATION_API_URL = config("AUTOMATION_API_URL", default="")
# Internal-tier service token (A0), sent as `Authorization: Bearer`. Same env
# name as keep-automation-api. Never logged.
INTERNAL_SERVICE_TOKEN = config("INTERNAL_SERVICE_TOKEN", default="")

# Submit acknowledges in seconds (early ack, spec §7.1). Clamped so connect +
# write + read (httpx applies the timeout per phase) stays under the 30s
# provisional cooldown TTL even at the caps (5 + 12 + 12 = 29s, spec §7.6): an
# `accepted` that arrives after the claim expired cannot arm the cooldown.
AUTOMATION_API_CONNECT_TIMEOUT_SECONDS = min(
    5.0,
    max(0.1, config("AUTOMATION_API_CONNECT_TIMEOUT_SECONDS", default="2.0", cast=float)),
)
AUTOMATION_API_TIMEOUT_SECONDS = min(
    12.0,
    max(1.0, config("AUTOMATION_API_TIMEOUT_SECONDS", default="10.0", cast=float)),
)

# Bound on waiting for in-flight submits when partitions are revoked: one
# attempt's worst case (connect + write + read) plus margin. Far below
# `max.poll.interval.ms`. Shutdown drains the same work, so the pod's
# `terminationGracePeriodSeconds` must exceed this by a few seconds (~35s+ at
# the defaults; the k8s default of 30s is too short).
IN_FLIGHT_DRAIN_TIMEOUT_SECONDS = (
    AUTOMATION_API_CONNECT_TIMEOUT_SECONDS + 2 * AUTOMATION_API_TIMEOUT_SECONDS + 5.0
)

# Backoff between attempts of an unacknowledged submit (429 capacity, 5xx,
# transport error). Capped exponential with jitter; retries continue until the
# message settles, the consumer stops, or its partition is revoked.
# Floored above zero: 200 workers retrying with no delay is a storm against
# exactly the API that is struggling.
SUBMIT_RETRY_BACKOFF_INITIAL_SECONDS = max(
    0.1, config("SUBMIT_RETRY_BACKOFF_INITIAL_SECONDS", default="1.0", cast=float)
)
SUBMIT_RETRY_BACKOFF_MAX_SECONDS = max(
    SUBMIT_RETRY_BACKOFF_INITIAL_SECONDS,
    config("SUBMIT_RETRY_BACKOFF_MAX_SECONDS", default="30.0", cast=float),
)
