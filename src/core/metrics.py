"""Prometheus metrics for the consumer (skeleton set)."""
from enum import Enum

from prometheus_client import Counter, Gauge

from src.models.idempotency import IdempotencyOutcome


class RedisOperation(str, Enum):
    CLAIM = "claim"
    GET = "get"
    MARK_DONE = "mark_done"


class GateConfigSetting(str, Enum):
    REDIS_URL = "redis_url"
    REDIS_CLIENT = "redis_client"

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

# Set at startup: 1 when a gate dependency is not configured at all. Without it,
# a consumer deployed with no REDIS_URL runs 100% ungated and looks identical to
# a healthy one — `redis_errors` never fires, because nothing is ever attempted.
# Mirrors keep-event-handler's `automation_index_config_missing`.
gates_config_missing = Gauge(
    "keep_automation_consumer_gates_config_missing",
    "1 when a gate dependency is unconfigured (gates then fail open)",
    ["setting"],
)

# Open and cache label children up front. `prometheus_client` does not emit a labelled
# child until it is first touched, so an alert written as
# `rate(redis_errors{operation="claim"}[5m]) > 0` reads "no data" for
# not-deployed, no-traffic AND misconfigured alike. Pre-initialising makes an
# absent series mean "not scraped", nothing else.
redis_error_counters = {
    operation: redis_errors.labels(operation=operation.value)
    for operation in RedisOperation
}
idempotency_decision_counters = {
    outcome: idempotency_decisions.labels(outcome=outcome.value)
    for outcome in IdempotencyOutcome
}
gate_config_missing_gauges = {
    setting: gates_config_missing.labels(setting=setting.value)
    for setting in GateConfigSetting
}

worker_pool_saturated = Gauge(
    "keep_automation_consumer_worker_pool_saturated",
    "1 while Kafka partitions are paused because every worker is occupied",
)
worker_pool_saturation_events = Counter(
    "keep_automation_consumer_worker_pool_saturation_events_total",
    "Transitions into worker-pool saturation",
)
worker_pool_tasks_in_flight = Gauge(
    "keep_automation_consumer_worker_pool_tasks_in_flight",
    "Per-message tasks currently occupying worker slots",
)

# Messages whose processing raised. Today the worker guard logs and moves on, and
# the offset is uncommitted so the message is redelivered — but C11 commits
# inside that same path, at which point this counter is the only evidence a
# message was skipped. Added with the guard, not after it.
handle_errors = Counter(
    "keep_automation_consumer_handle_errors_total",
    "Matched-alert messages whose processing raised and was skipped",
)
