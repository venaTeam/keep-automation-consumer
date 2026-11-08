"""Prometheus metrics for the consumer (skeleton set)."""
from enum import Enum

from prometheus_client import Counter, Gauge, Histogram

from src.models.cooldown import CooldownOutcome
from src.models.idempotency import IdempotencyOutcome
from src.models.submit import SubmitOutcome


class RedisOperation(str, Enum):
    CLAIM = "claim"
    GET = "get"
    MARK_DONE = "mark_done"
    COOLDOWN_CLAIM = "cooldown_claim"
    COOLDOWN_TTL = "cooldown_ttl"
    COOLDOWN_ACTIVATE = "cooldown_activate"
    COOLDOWN_RELEASE = "cooldown_release"
    COOLDOWN_RECLAIM = "cooldown_reclaim"


class GateConfigSetting(str, Enum):
    REDIS_URL = "redis_url"
    REDIS_CLIENT = "redis_client"


class FailOpenReason(str, Enum):
    """Why a submit went out without the idempotency gate (`gate_flags.reason`)."""

    REDIS_UNAVAILABLE = "redis_unavailable"
    MISSING_HISTORY_ID = "missing_history_id"


class AbandonReason(str, Enum):
    """Why a worker stopped retrying a message without settling it."""

    SHUTDOWN = "shutdown"
    REVOKED = "revoked"

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

# Additive metric: preserve the existing outcome series used by dashboards.
idempotency_fail_open = Counter(
    "keep_automation_consumer_idempotency_fail_open_total",
    "Idempotency fail-open decisions by bounded reason",
    ["reason"],
)
idempotency_fail_open_counters = {
    reason: idempotency_fail_open.labels(reason=reason)
    for reason in ("missing_history_id", "redis_unavailable")
}

cooldown_decisions = Counter(
    "keep_automation_consumer_cooldown_decisions_total",
    "Cooldown-gate decisions by outcome",
    ["outcome"],
)

# The Redis-down signal. Spec §6.3 requires fail-open to page loudly — this is
# the counter that alert reads (`fail_open_submits` below is the submit-side view).
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
cooldown_decision_counters = {
    outcome: cooldown_decisions.labels(outcome=outcome.value)
    for outcome in CooldownOutcome
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

# Messages whose processing raised outside the submit pipeline. The worker guard logs, settles the offset
# (it is committed past — a raising message would otherwise block its
# partition's contiguous commits forever) and moves on; this counter is the only
# evidence a message was skipped.
handle_errors = Counter(
    "keep_automation_consumer_handle_errors_total",
    "Matched-alert messages whose processing raised and was skipped",
)

# -- Submit (C11) -------------------------------------------------------------

# One sample per submit attempt, by classified outcome (contracts §Submit,
# "Consumer response classification"). Non-acknowledged outcomes are retried,
# except `rejected`, which is skipped.
submits = Counter(
    "keep_automation_consumer_submits_total",
    "Submit attempts by classified outcome",
    ["outcome"],
)

# The distinct capacity-backoff signal (story C11, spec §9.2): the API's
# in-flight ceiling is saturated and alerts are waiting in Kafka. Sustained
# non-zero rate pages us (spec §9.3).
submit_capacity_backoffs = Counter(
    "keep_automation_consumer_submit_capacity_backoffs_total",
    "Submits rejected with 429 capacity and backed off",
)

# A rejected submit is a matched alert whose automation will NOT run: the API
# refused it for good and the offset was committed past it. Any rate > 0 is an
# incident — one bad message, or an API/contract regression refusing them all.
submit_rejections = Counter(
    "keep_automation_consumer_submit_rejected_total",
    "Submits definitively refused by the API (skipped, offset committed)",
    ["status"],
)

# The submit-side Redis-down view (spec §6.3, §9.3 "Redis down / fail-open
# submits occurring → us, loudly"): every submit sent with the idempotency gate
# skipped. Unlike `redis_errors`, this keeps scaling with traffic while the
# breaker is open.
fail_open_submits = Counter(
    "keep_automation_consumer_fail_open_submits_total",
    "Submits sent without the idempotency gate, by reason",
    ["reason"],
)

submit_duration = Histogram(
    "keep_automation_consumer_submit_duration_seconds",
    "Wall time of one POST /internal/submit attempt (acknowledgement latency)",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0),
)

# Messages a worker stopped retrying without settling. The offset stays
# uncommitted, so Kafka redelivers them — this counts work handed back, not loss.
messages_abandoned = Counter(
    "keep_automation_consumer_messages_abandoned_total",
    "Unsettled messages handed back to Kafka (shutdown or partition revoked)",
    ["reason"],
)

# Set at startup: 1 when the submit path is missing a setting. `service_token`
# alone does not stop the consumer (the API does not verify it yet), but once
# D17 enforces it every submit becomes a retried 401.
submit_config_missing = Gauge(
    "keep_automation_consumer_submit_config_missing",
    "1 when a submit-path setting is unconfigured",
    ["setting"],
)

submit_outcome_counters = {
    outcome: submits.labels(outcome=outcome.value) for outcome in SubmitOutcome
}
fail_open_submit_counters = {
    reason: fail_open_submits.labels(reason=reason.value) for reason in FailOpenReason
}
messages_abandoned_counters = {
    reason: messages_abandoned.labels(reason=reason.value) for reason in AbandonReason
}
# Label is the refusing HTTP status, or `unsendable` when no request could
# carry the message at all.
submit_rejection_counters = {
    status: submit_rejections.labels(status=status)
    for status in ("400", "404", "409", "422", "unsendable")
}

# Unexpected exceptions inside one submit attempt: the claim is released and
# the attempt retried like a clean API failure — never committed past.
pipeline_errors = Counter(
    "keep_automation_consumer_pipeline_errors_total",
    "Submit attempts that raised unexpectedly (retried, not skipped)",
)
service_token_missing = submit_config_missing.labels(setting="service_token")

# -- Offsets (C11) ------------------------------------------------------------

offsets_committed = Counter(
    "keep_automation_consumer_offset_commits_total",
    "Partition offset commits issued (async periodic, revoke, shutdown)",
)
offset_commit_errors = Counter(
    "keep_automation_consumer_offset_commit_errors_total",
    "Partition offset commits that failed (retried on the next commit tick)",
)

# Partitions paused because one stuck message holds back too many settled ones
# behind it (MAX_UNCOMMITTED_OFFSETS_PER_PARTITION). Other partitions keep
# flowing; a non-zero value names a stuck automation, not a slow consumer.
partitions_paused_uncommitted = Gauge(
    "keep_automation_consumer_partitions_paused_uncommitted_window",
    "Partitions paused because their uncommitted offset window is full",
)

# A revoke waits for in-flight work on the revoked partitions before
# committing; this counts revokes that gave up at IN_FLIGHT_DRAIN_TIMEOUT_SECONDS
# (the work is then redelivered to the next owner while it may still run here).
revoke_drain_timeouts = Counter(
    "keep_automation_consumer_revoke_drain_timeouts_total",
    "Partition revokes that stopped waiting for in-flight submits",
)

# A message polled while its partition was paused or no worker was free is
# rewound (seek) and fetched again later — never dispatched, never dropped.
messages_rewound = Counter(
    "keep_automation_consumer_messages_rewound_total",
    "Polled messages rewound because they could not be dispatched yet",
)
