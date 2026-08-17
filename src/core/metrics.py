"""Prometheus metrics for the consumer (skeleton set)."""
from prometheus_client import Counter

messages_consumed = Counter(
    "keep_automation_consumer_messages_consumed_total",
    "Matched-alert messages consumed from the topic",
)
deserialize_errors = Counter(
    "keep_automation_consumer_deserialize_errors_total",
    "Matched-alert messages that failed to deserialize",
)

# Gates (spec §4.5, §5.2). `outcome` mirrors IdempotencyOutcome:
#   claimed   - first sight of this (history_id, automation_id); submit
#   duplicate - a previous submit was API-confirmed; suppress + audit
#   ambiguous - key present but not `done` (or vanished); submit anyway
#   fail_open - Redis unreachable/unset; submit without the gate
idempotency_decisions = Counter(
    "keep_automation_consumer_idempotency_decisions_total",
    "Idempotency-gate decisions by outcome",
    ["outcome"],
)

# The Redis-down signal. Spec §6.3 requires fail-open to page loudly — this is
# the counter that alert reads (the C11 pipeline adds the submit-side view).
redis_errors = Counter(
    "keep_automation_consumer_redis_errors_total",
    "Redis operations that failed (gates then fail open)",
    ["operation"],
)
