"""Idempotency gate (C9) — spec §4.5, §5.2 step 1, §6.1 #1.

Per matched message:

    SET idem:{history_id}:{automation_id} "pending" NX EX 24h

| Redis says | Outcome | What the pipeline does |
|---|---|---|
| NX succeeded | `CLAIMED` | submit |
| NX failed, value `done` | `DUPLICATE` | audit `suppressed`(duplicate), commit offset, no submit |
| NX failed, value `pending` | `AMBIGUOUS` | **submit** |
| NX failed, key already gone | `AMBIGUOUS` | **submit** |
| unreachable / not configured | `FAIL_OPEN` | **submit**, `gate_flags={idempotency: skipped, reason: redis_unavailable}` |

The three "submit" rows are the whole point of the design and are not
conservatism: this gate is an **optimisation**, never the authority. The
authority is the DB unique constraint on `(history_id, automation_id)` in
`keep-automation-api`, which short-circuits a duplicate to `200 already
submitted` **before** `/run` is ever called (§6.1 #1). Dropping a message on
ambiguity would therefore trade a cheap, already-handled duplicate for a
silently lost automation — the one failure this system refuses.

Only an API-**confirmed** submit writes `done` (`mark_done`, called by C11).
There is deliberately no `release()`: a clean API failure leaves the key at
`pending`, which the table above already routes to a submit on redelivery. The
provisional-then-release dance belongs to the cooldown key (C10), whose failure
mode — suppressing a *different* event — is not covered by any DB constraint.

Nothing here raises. Every Redis error becomes `FAIL_OPEN` plus a metric — and
after `REDIS_BREAKER_FAILURE_THRESHOLD` consecutive failures the breaker
short-circuits without touching the socket, because socket timeouts bound one
message's latency but not the loop's throughput.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from src.config.consts import (
    IDEMPOTENCY_TTL_SECONDS,
    REDIS_BREAKER_FAILURE_THRESHOLD,
    REDIS_BREAKER_OPEN_SECONDS,
    REDIS_ERROR_LOG_INTERVAL_SECONDS,
)
from src.core.metrics import idempotency_decisions, redis_errors
from src.core.redis_client import get_redis_client
from src.models.matched_message import MatchedAlertMessage

logger = logging.getLogger(__name__)

# Key value states (contracts §Redis keys — Idempotency).
VALUE_PENDING = "pending"
VALUE_DONE = "done"

# Reason token recorded on a fail-open submit (spec §6.3, C11's `gate_flags`).
REASON_REDIS_UNAVAILABLE = "redis_unavailable"


class IdempotencyOutcome(str, Enum):
    CLAIMED = "claimed"
    DUPLICATE = "duplicate"
    AMBIGUOUS = "ambiguous"
    FAIL_OPEN = "fail_open"


@dataclass(frozen=True)
class IdempotencyDecision:
    outcome: IdempotencyOutcome
    key: str
    gate_flags: dict = field(default_factory=dict)

    @property
    def should_submit(self) -> bool:
        """Everything except a confirmed duplicate proceeds to submit."""
        return self.outcome is not IdempotencyOutcome.DUPLICATE


def idempotency_key(history_id: str, automation_id: str) -> str:
    """`idem:{history_id}:{automation_id}` — contracts §Redis keys, verbatim.

    Keyed on `history_id` (unique per alert **event**), never on `fingerprint`:
    the gate suppresses redeliveries of one event, not re-fires of one logical
    alert. Re-fire suppression is the cooldown key's job (C10), on its own axis.
    """
    return f"idem:{history_id}:{automation_id}"


class IdempotencyGate:
    def __init__(self, client_factory=get_redis_client, clock=time.monotonic):
        self._client_factory = client_factory
        self._clock = clock
        self._consecutive_failures = 0
        self._breaker_open_until = 0.0
        self._next_error_log_at = 0.0

    # -- circuit breaker ---------------------------------------------------
    #
    # Deliberately lock-free. Under the C11 worker pool several threads race on
    # these two attributes, and the worst outcome of a lost update is one extra
    # probe or one extra skipped call — the gate is advisory either way, so a
    # lock on the hot path would cost more than the race.

    @property
    def _breaker_is_open(self) -> bool:
        return self._clock() < self._breaker_open_until

    def _record_success(self) -> None:
        self._consecutive_failures = 0
        self._breaker_open_until = 0.0

    def _record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= REDIS_BREAKER_FAILURE_THRESHOLD:
            self._breaker_open_until = self._clock() + REDIS_BREAKER_OPEN_SECONDS
            # Half-open: the counter resets with the window, so the first
            # message after it expires probes Redis for real, and a single
            # further failure re-opens the breaker immediately.
            self._consecutive_failures = 0

    def _log_redis_error(self, operation: str, key: str) -> None:
        """Budgeted traceback: the metric counts every failure, the log doesn't.

        A Redis outage at ~200 msg/s writes 200 stack traces/s synchronously
        from inside the poll loop otherwise — the noise itself becomes the
        outage. Same reasoning keep-event-handler's pubsub listener applies to
        its reconnect loop.
        """
        now = self._clock()
        if now >= self._next_error_log_at:
            self._next_error_log_at = now + REDIS_ERROR_LOG_INTERVAL_SECONDS
            logger.exception(
                "automations: idempotency %s failed for key=%s; failing open "
                "(further tracebacks suppressed for %ss)",
                operation,
                key,
                REDIS_ERROR_LOG_INTERVAL_SECONDS,
            )
        else:
            logger.debug("automations: idempotency %s failed for key=%s", operation, key)

    def claim(self, message: MatchedAlertMessage) -> IdempotencyDecision:
        history_id = message.history_id
        key = idempotency_key(history_id, message.automation_id)

        if not history_id:
            # The matcher stamps `history_id` on every alert (contracts §Alert
            # payload: always present). Without it the key would collapse to
            # `idem:None:{automation_id}` and one automation's whole event
            # stream would share a single gate — the first event suppressing
            # every later one. Fail open instead: the DB constraint still
            # dedups, and submit is the safe side.
            logger.warning(
                "automations: matched message has no history_id "
                "(automation_id=%s tenant_id=%s); idempotency gate skipped",
                message.automation_id,
                message.tenant_id,
            )
            return self._decide(IdempotencyOutcome.FAIL_OPEN, key, "missing_history_id")

        if self._breaker_is_open:
            # Redis is known-sick; skip the socket entirely rather than pay a
            # timeout per message. Counted as fail-open, not as a new error.
            return self._decide(
                IdempotencyOutcome.FAIL_OPEN, key, REASON_REDIS_UNAVAILABLE
            )

        try:
            # The factory is inside the try on purpose: `get_redis_client()`
            # swallows its own failures today, but this gate must not depend on
            # a *different* module's guarantee to keep its "nothing raises"
            # contract — an injected factory (C11, tests) may raise.
            client = self._client_factory()
            if client is None:
                return self._decide(
                    IdempotencyOutcome.FAIL_OPEN, key, REASON_REDIS_UNAVAILABLE
                )
            claimed = client.set(
                key, VALUE_PENDING, nx=True, ex=IDEMPOTENCY_TTL_SECONDS
            )
        except Exception:  # noqa: BLE001 - a gate never fails the message
            redis_errors.labels(operation="claim").inc()
            self._record_failure()
            self._log_redis_error("claim", key)
            return self._decide(
                IdempotencyOutcome.FAIL_OPEN, key, REASON_REDIS_UNAVAILABLE
            )

        self._record_success()
        if claimed:
            return self._decide(IdempotencyOutcome.CLAIMED, key)

        # NX lost the race with an earlier delivery of this same event; the
        # stored value says whether that delivery ever reached a confirmed
        # submit. A separate GET (rather than one atomic op) is safe here
        # because both branches are non-destructive.
        try:
            value = client.get(key)
        except Exception:  # noqa: BLE001
            redis_errors.labels(operation="get").inc()
            self._record_failure()
            self._log_redis_error("read", key)
            return self._decide(
                IdempotencyOutcome.FAIL_OPEN, key, REASON_REDIS_UNAVAILABLE
            )

        value = _as_text(value)
        if value == VALUE_DONE:
            return self._decide(IdempotencyOutcome.DUPLICATE, key)

        # `pending` (an in-flight or crashed sibling) or gone (expired between
        # the SET and the GET). Both submit — see the module docstring.
        return self._decide(IdempotencyOutcome.AMBIGUOUS, key)

    def mark_done(self, message: MatchedAlertMessage) -> bool:
        """Flip the key to `done` after an **API-confirmed** submit (C11).

        Re-arms the full 24h TTL from the moment the run became durable, so the
        suppression window covers redeliveries counted from that point rather
        than from the claim. Returns False when Redis could not be written —
        the caller commits the offset regardless: the run is already recorded
        in Postgres, and a lost `done` costs one extra idempotent submit.
        """
        history_id = message.history_id
        if not history_id or self._breaker_is_open:
            return False

        key = idempotency_key(history_id, message.automation_id)
        try:
            client = self._client_factory()
            if client is None:
                return False
            client.set(key, VALUE_DONE, ex=IDEMPOTENCY_TTL_SECONDS)
        except Exception:  # noqa: BLE001
            redis_errors.labels(operation="mark_done").inc()
            self._record_failure()
            self._log_redis_error("mark_done", key)
            # A redelivery will re-submit and the DB constraint will absorb it.
            return False

        self._record_success()
        return True

    @staticmethod
    def _decide(
        outcome: IdempotencyOutcome, key: str, reason: Optional[str] = None
    ) -> IdempotencyDecision:
        idempotency_decisions.labels(outcome=outcome.value).inc()
        flags = {"idempotency": "skipped" if reason else outcome.value}
        if reason:
            flags["reason"] = reason
        return IdempotencyDecision(outcome=outcome, key=key, gate_flags=flags)


def _as_text(value) -> Optional[str]:
    """Tolerate a client built without `decode_responses` (tests, reuse)."""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value
