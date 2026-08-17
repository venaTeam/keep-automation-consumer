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

Nothing here raises. Every Redis error becomes `FAIL_OPEN` plus a metric.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from src.config.consts import IDEMPOTENCY_TTL_SECONDS
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
    def __init__(self, client_factory=get_redis_client):
        self._client_factory = client_factory

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

        client = self._client_factory()
        if client is None:
            return self._decide(
                IdempotencyOutcome.FAIL_OPEN, key, REASON_REDIS_UNAVAILABLE
            )

        try:
            claimed = client.set(
                key, VALUE_PENDING, nx=True, ex=IDEMPOTENCY_TTL_SECONDS
            )
        except Exception:  # noqa: BLE001 - a gate never fails the message
            redis_errors.labels(operation="claim").inc()
            logger.exception(
                "automations: idempotency claim failed for key=%s; failing open", key
            )
            return self._decide(
                IdempotencyOutcome.FAIL_OPEN, key, REASON_REDIS_UNAVAILABLE
            )

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
            logger.exception(
                "automations: idempotency read failed for key=%s; failing open", key
            )
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
        if not history_id:
            return False

        client = self._client_factory()
        if client is None:
            return False

        key = idempotency_key(history_id, message.automation_id)
        try:
            client.set(key, VALUE_DONE, ex=IDEMPOTENCY_TTL_SECONDS)
            return True
        except Exception:  # noqa: BLE001
            redis_errors.labels(operation="mark_done").inc()
            logger.exception(
                "automations: could not mark idempotency key=%s done; a redelivery "
                "will re-submit and the DB constraint will absorb it",
                key,
            )
            return False

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
