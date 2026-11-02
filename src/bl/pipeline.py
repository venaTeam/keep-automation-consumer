"""Per-message submit orchestration (C11) — spec §5.2, implemented verbatim.

One attempt is the whole sequence a Kafka redelivery would run:

    idempotency claim → cooldown claim → POST /internal/submit → gate lifecycle

| Result | Idem key | `run_id`-owned cooldown claim | Settled (offset commits)? |
|---|---|---|---|
| idem `duplicate` | — | not claimed | yes — audit `duplicate` |
| cooldown `suppressed` | — | the other event's, untouched | yes — audit `cooldown` |
| 200 `accepted`, or `already_submitted` with our `run_id` + recent (claim re-entered, or a prior attempt) | → `done` | compare-and-extend (armed) | yes |
| any other 200 `already_submitted` (stale) / `suppressed` | → `done` | compare-and-delete | yes |
| 429 `capacity` | stays `pending` | compare-and-delete | no — retry (+ capacity metric) |
| 5xx / 408 / 401 / 403 / transport / invalid 200 | stays `pending` | compare-and-delete | no — retry |
| 400 / 404 / 409 / 422 + typed refusal body | stays `pending` | compare-and-delete | yes — skipped, loud |
| body unencodable (NaN in `alert`) — nothing sent | stays `pending` | compare-and-delete | yes — skipped, loud |
| unexpected exception in an attempt | stays `pending` | compare-and-delete | no — retry, counted |

Redis unavailable at the idempotency gate ⇒ the cooldown gate is skipped
without a Redis call, and the submit carries `gate_flags` saying so (spec §6.3:
"submit without gates", audited).

`run()` repeats attempts with capped exponential backoff until one settles. That
is the "back off / redeliver" of the failure table, done inside the worker so
the Kafka poll thread never waits (ADR-007: no retry may stall polling past
`max.poll.interval.ms`). Re-running the gates per attempt keeps redelivery
semantics exact: the provisional cooldown claim was released on failure, so the
next attempt has to win it again — or learn that another event now holds it.

The `run_id` is derived per event — `uuid5` over `automation_id:history_id`
(contracts §Submit) — so every attempt and every redelivery of one event carries
the same one: it is the cooldown ownership token and the submit's trace id. A
redelivery after a crash therefore re-enters the provisional claim its dead
delivery left instead of being suppressed by it. On an `already_submitted` the
API's `run_id` (the existing row's) is authoritative.
"""

import logging
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from src.bl.gates.cooldown import CooldownGate
from src.bl.gates.idempotency import REASON_REDIS_UNAVAILABLE, IdempotencyGate
from src.bl.submit_client import SubmitClient
from src.bl.suppression_audit import (
    REASON_COOLDOWN,
    REASON_DUPLICATE,
    SuppressionAuditor,
)
from src.config.consts import (
    REDIS_ERROR_LOG_INTERVAL_SECONDS,
    SUBMIT_RETRY_BACKOFF_INITIAL_SECONDS,
    SUBMIT_RETRY_BACKOFF_MAX_SECONDS,
)
from src.core.metrics import (
    AbandonReason,
    FailOpenReason,
    fail_open_submit_counters,
    messages_abandoned_counters,
    pipeline_errors,
    submit_capacity_backoffs,
    submit_duration,
    submit_outcome_counters,
    submit_rejection_counters,
    submit_rejections,
)
from src.models.cooldown import CooldownDecision, CooldownOutcome
from src.models.idempotency import IdempotencyOutcome
from src.models.matched_message import MatchedAlertMessage
from src.models.submit import (
    SubmitOutcome,
    SubmitRequest,
    SubmitResult,
    derive_run_id,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AttemptResult:
    # True when the message is finished with — its offset may be committed.
    settled: bool
    # What happened, for logs and tests: `duplicate`, `cooldown_suppressed`, or
    # a `SubmitOutcome` value.
    outcome: str


def _always_owned() -> bool:
    return True


class SubmitPipeline:
    def __init__(
        self,
        *,
        idempotency_gate: IdempotencyGate,
        cooldown_gate: CooldownGate,
        submit_client: SubmitClient,
        suppression_auditor: SuppressionAuditor,
        backoff_initial_seconds: float = SUBMIT_RETRY_BACKOFF_INITIAL_SECONDS,
        backoff_max_seconds: float = SUBMIT_RETRY_BACKOFF_MAX_SECONDS,
        random_fraction: Callable[[], float] = random.random,
        monotonic_clock: Callable[[], float] = time.monotonic,
        retry_log_interval_seconds: float = REDIS_ERROR_LOG_INTERVAL_SECONDS,
    ) -> None:
        self._idempotency_gate = idempotency_gate
        self._cooldown_gate = cooldown_gate
        self._submit_client = submit_client
        self._suppression_auditor = suppression_auditor
        self._backoff_initial_seconds = backoff_initial_seconds
        self._backoff_max_seconds = max(backoff_initial_seconds, backoff_max_seconds)
        self._random_fraction = random_fraction
        self._monotonic_clock = monotonic_clock
        self._retry_log_interval_seconds = max(0.0, retry_log_interval_seconds)
        self._next_retry_log_at = 0.0
        self._log_lock = threading.Lock()

    # -- retry loop -----------------------------------------------------------

    def run(
        self,
        message: MatchedAlertMessage,
        *,
        cancelled: threading.Event,
        still_owned: Callable[[], bool] = _always_owned,
    ) -> Optional[AttemptResult]:
        """Attempt until the message settles; None when handed back to Kafka.

        `cancelled` is set on shutdown and on revoke of the message's partition
        (it also cuts a backoff sleep short); `still_owned` reports whether this
        consumer still holds the partition. Either one ending abandons the
        message unsettled: its offset is never committed,
        so Kafka redelivers it (to this process after a restart, or to the new
        owner after a rebalance) and the idempotent sequence runs again there.
        """
        run_id = derive_run_id(message.automation_id, message.history_id)
        attempt = 0
        while True:
            abandon_reason = self._abandon_reason(cancelled, still_owned)
            if abandon_reason is not None:
                self._abandon(message, abandon_reason)
                return None

            try:
                result = self.process_once(
                    message, run_id=run_id, prior_attempt=attempt > 0
                )
            except Exception:  # noqa: BLE001
                # A bug or an unforeseen library error: the attempt already
                # released its claim. Retrying is recoverable once fixed;
                # committing past the message would lose it for good.
                pipeline_errors.inc()
                self._log_attempt_error(message)
                result = AttemptResult(settled=False, outcome="error")
            if result.settled:
                return result

            attempt += 1
            delay = self._backoff_delay(attempt)
            self._log_retry(message, result, attempt, delay)
            # Event.wait doubles as an interruptible sleep: shutdown does not
            # wait out a 30s backoff.
            cancelled.wait(delay)

    @staticmethod
    def _abandon_reason(
        cancelled: threading.Event, still_owned: Callable[[], bool]
    ) -> Optional[AbandonReason]:
        # Ownership first: a revoke also sets `cancelled` (to wake the backoff
        # sleep), and must be counted as a revoke, not a shutdown.
        if not still_owned():
            return AbandonReason.REVOKED
        if cancelled.is_set():
            return AbandonReason.SHUTDOWN
        return None

    def _backoff_delay(self, attempt: int) -> float:
        """Capped exponential backoff with equal jitter.

        Half the cap is fixed so a retry storm never collapses to zero delay;
        the other half is random so 200 workers that failed together against a
        down API do not come back in lockstep.
        """
        cap = min(
            self._backoff_max_seconds,
            self._backoff_initial_seconds * (2 ** min(attempt - 1, 32)),
        )
        return cap / 2 + self._random_fraction() * cap / 2

    # -- one attempt ------------------------------------------------------------

    def process_once(
        self,
        message: MatchedAlertMessage,
        *,
        run_id: str,
        prior_attempt: bool = False,
    ) -> AttemptResult:
        """One attempt. `prior_attempt`: an earlier attempt of this delivery
        went unacknowledged — it may have reached the API and been accepted
        with its response lost, so an `already_submitted` now is recent."""
        idempotency = self._idempotency_gate.claim(message)
        if idempotency.outcome is IdempotencyOutcome.DUPLICATE:
            # A previous delivery of this same event reached an API
            # acknowledgement. Nothing is sent; the suppression is audited so it
            # is visible rather than silent (spec §7.5).
            self._audit(message, REASON_DUPLICATE, idempotency.gate_flags)
            return AttemptResult(settled=True, outcome="duplicate")

        if idempotency.reason == REASON_REDIS_UNAVAILABLE:
            cooldown = self._cooldown_gate.skip_unavailable(message)
        else:
            cooldown = self._cooldown_gate.claim(message, run_id=run_id)

        gate_flags = {**idempotency.gate_flags, **cooldown.gate_flags}
        if cooldown.outcome is CooldownOutcome.SUPPRESSED:
            self._audit(
                message,
                REASON_COOLDOWN,
                gate_flags,
                detail=_cooldown_detail(cooldown),
            )
            return AttemptResult(settled=True, outcome="cooldown_suppressed")

        try:
            return self._submit_and_settle(
                message, run_id=run_id, gate_flags=gate_flags,
                idempotency=idempotency, cooldown=cooldown,
                prior_attempt=prior_attempt,
            )
        except BaseException:
            # Never leave a provisional claim behind an attempt that blew up:
            # it would suppress the entity — or this message's own retry —
            # for 30s with nothing accepted. A no-op once armed or released.
            self._cooldown_gate.release(cooldown)
            raise

    def _submit_and_settle(
        self,
        message: MatchedAlertMessage,
        *,
        run_id: str,
        gate_flags: dict[str, str],
        idempotency,
        cooldown: CooldownDecision,
        prior_attempt: bool,
    ) -> AttemptResult:
        if idempotency.outcome is IdempotencyOutcome.FAIL_OPEN:
            self._count_fail_open_submit(idempotency.reason)

        result = self._submit(
            SubmitRequest(
                tenant_id=message.tenant_id,
                automation_id=message.automation_id,
                history_id=message.history_id,
                fingerprint=message.fingerprint,
                run_id=run_id,
                alert=message.alert,
                gate_flags=gate_flags,
            )
        )

        if result.acknowledged:
            # The run row is durable: `done` turns a redelivery of this event
            # into an audited duplicate instead of another submit. A failed
            # write costs one extra idempotent submit, never correctness.
            self._idempotency_gate.mark_done(message)
            if result.outcome is SubmitOutcome.ACCEPTED or _is_recent_own_run(
                result, run_id, cooldown=cooldown, prior_attempt=prior_attempt
            ):
                self._arm_cooldown(message, cooldown, run_id, result)
            else:
                # `suppressed`, or a stale `already_submitted` (an offset
                # replay, a copy past the idempotency TTL): arming would restart
                # the entity's window for a run that may be hours old.
                self._cooldown_gate.release(cooldown)
            return AttemptResult(settled=True, outcome=result.outcome.value)

        # Not acknowledged: give back this message's still-provisional claim so
        # it does not suppress the entity for 30s with nothing accepted.
        self._cooldown_gate.release(cooldown)

        if result.outcome is SubmitOutcome.CAPACITY:
            submit_capacity_backoffs.inc()
        elif result.outcome in (SubmitOutcome.REJECTED, SubmitOutcome.UNSENDABLE):
            self._record_rejection(message, result, run_id)
            return AttemptResult(settled=True, outcome=result.outcome.value)
        return AttemptResult(settled=False, outcome=result.outcome.value)

    def _submit(self, request: SubmitRequest) -> SubmitResult:
        started = self._monotonic_clock()
        try:
            result = self._submit_client.submit(request)
        finally:
            submit_duration.observe(max(0.0, self._monotonic_clock() - started))
        submit_outcome_counters[result.outcome].inc()
        return result

    def _arm_cooldown(
        self,
        message: MatchedAlertMessage,
        cooldown: CooldownDecision,
        run_id: str,
        result: SubmitResult,
    ) -> None:
        armed = self._cooldown_gate.activate(cooldown)
        if cooldown.outcome is CooldownOutcome.CLAIMED and not armed:
            # The provisional claim expired, was released by a sibling delivery
            # of this event, or Redis failed before the acknowledgement arrived,
            # so this run is not covering its entity. Best-effort by design: the
            # run itself is durable. On `already_submitted` the usual cause is a
            # sibling that armed it first — expected, so not a warning.
            log = (
                logger.warning
                if result.outcome is SubmitOutcome.ACCEPTED
                else logger.debug
            )
            log(
                "automations: accepted run could not arm its cooldown "
                "(claim no longer owned) tenant_id=%s automation_id=%s run_id=%s",
                message.tenant_id,
                message.automation_id,
                run_id,
            )

    @staticmethod
    def _count_fail_open_submit(reason: Optional[str]) -> None:
        try:
            counter = fail_open_submit_counters[FailOpenReason(reason)]
        except ValueError:
            counter = fail_open_submit_counters[FailOpenReason.REDIS_UNAVAILABLE]
        counter.inc()

    @staticmethod
    def _record_rejection(
        message: MatchedAlertMessage, result: SubmitResult, run_id: str
    ) -> None:
        label = (
            "unsendable"
            if result.outcome is SubmitOutcome.UNSENDABLE
            else str(result.http_status)
        )
        counter = submit_rejection_counters.get(label)
        if counter is None:
            counter = submit_rejections.labels(status=label)
        counter.inc()
        # Logged unbudgeted, at ERROR: each line is the only record of a
        # matched alert whose automation will not run.
        logger.error(
            "automations: submit definitively refused (%s); skipping the "
            "message, its automation will NOT run tenant_id=%s automation_id=%s "
            "history_id=%s run_id=%s",
            label if label == "unsendable" else f"http {label}",
            message.tenant_id,
            message.automation_id,
            message.history_id,
            run_id,
        )

    def _audit(
        self,
        message: MatchedAlertMessage,
        reason: str,
        gate_flags: dict[str, str],
        *,
        detail: Optional[dict[str, Any]] = None,
    ) -> None:
        try:
            self._suppression_auditor.record_suppression(
                tenant_id=message.tenant_id,
                automation_id=message.automation_id,
                history_id=message.history_id,
                fingerprint=message.fingerprint,
                reason=reason,
                gate_flags=gate_flags,
                detail=detail,
            )
        except Exception:  # noqa: BLE001
            # D17 replaces the stub with an HTTP client that can fail. A failed
            # audit must not fail the message: the suppression decision stands,
            # and submitting instead is the worse outcome.
            logger.exception(
                "automations: could not audit a %s suppression "
                "tenant_id=%s automation_id=%s history_id=%s",
                reason,
                message.tenant_id,
                message.automation_id,
                message.history_id,
            )

    def _log_attempt_error(self, message: MatchedAlertMessage) -> None:
        """Budgeted traceback: a systematic bug would otherwise log per attempt."""
        now = self._monotonic_clock()
        with self._log_lock:
            should_log = now >= self._next_retry_log_at
            if should_log:
                self._next_retry_log_at = now + self._retry_log_interval_seconds
        if should_log:
            logger.exception(
                "automations: submit attempt raised; claim released, retrying "
                "tenant_id=%s automation_id=%s history_id=%s",
                message.tenant_id,
                message.automation_id,
                message.history_id,
            )

    def _abandon(self, message: MatchedAlertMessage, reason: AbandonReason) -> None:
        messages_abandoned_counters[reason].inc()
        logger.debug(
            "automations: message handed back to Kafka unsettled (%s) "
            "tenant_id=%s automation_id=%s history_id=%s",
            reason.value,
            message.tenant_id,
            message.automation_id,
            message.history_id,
        )

    def _log_retry(
        self,
        message: MatchedAlertMessage,
        result: AttemptResult,
        attempt: int,
        delay: float,
    ) -> None:
        """Budgeted: during an API outage every worker retries at once."""
        now = self._monotonic_clock()
        with self._log_lock:
            should_log = now >= self._next_retry_log_at
            if should_log:
                self._next_retry_log_at = now + self._retry_log_interval_seconds
        log = logger.warning if should_log else logger.debug
        log(
            "automations: submit not acknowledged (%s); offset left uncommitted, "
            "retrying in %.1fs (attempt %s) tenant_id=%s automation_id=%s "
            "history_id=%s",
            result.outcome,
            delay,
            attempt,
            message.tenant_id,
            message.automation_id,
            message.history_id,
        )


def _is_recent_own_run(
    result: SubmitResult,
    run_id: str,
    *,
    cooldown: CooldownDecision,
    prior_attempt: bool,
) -> bool:
    """`already_submitted` for this event, with evidence the run is recent.

    The run_id is derived per event, so carrying it proves only "this event
    ran", possibly hours ago. Recent means: this attempt re-entered a claim a
    delivery of the event held within the provisional TTL (a crash
    redelivery, a sibling in flight), or an earlier attempt of this delivery
    reached the API and its response was lost.
    """
    return (
        result.outcome is SubmitOutcome.ALREADY_SUBMITTED
        and result.response is not None
        and result.response.run_id == run_id
        and (cooldown.reentered or prior_attempt)
    )


def _cooldown_detail(decision: CooldownDecision) -> dict[str, Any]:
    return {
        "entity_hash": decision.entity_hash,
        "eligible_again_in_seconds": decision.eligible_again_in_seconds,
        "next_eligible_at": (
            decision.next_eligible_at.isoformat()
            if decision.next_eligible_at is not None
            else None
        ),
    }
