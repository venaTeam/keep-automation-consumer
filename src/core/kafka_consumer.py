"""Matched-alerts Kafka consumer.

Consumes the matched-alerts topic with **auto-commit off**, runs the idempotency
gate (C9) and logs the decision. Cooldown (C10) and submit + offset commit +
fail-open (C11) are still absent — the loop never commits offsets yet.

The gate runs inline in the poll loop, which is acceptable only because every
Redis call is bounded by `REDIS_SOCKET_*_TIMEOUT_SECONDS` and fails open: the
worst case is a fixed sub-second cost per message, not an unbounded stall
toward `max.poll.interval.ms`. C11 moves the whole per-message sequence
(gates → submit → commit) onto the worker pool, where the synchronous submit —
which blocks for the run's full duration — could never sit inline.
"""
import logging
import signal

from src.bl.gates.idempotency import IdempotencyGate, IdempotencyOutcome
from src.bl.suppression_audit import REASON_DUPLICATE, get_suppression_auditor
from src.config.consts import (
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_POLL_TIMEOUT_SECONDS,
    MATCHED_ALERTS_TOPIC,
)
from src.core.metrics import (
    deserialize_errors,
    gates_config_missing,
    messages_consumed,
)
from src.core.redis_client import redis_configured
from src.models.matched_message import MatchedAlertMessage

logger = logging.getLogger(__name__)


def build_consumer_config() -> dict:
    """confluent-kafka consumer config.

    `enable.auto.commit=False` is the contract: offsets are committed only after
    an API-confirmed submit (C11), never automatically.
    """
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "enable.auto.commit": False,
        "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
    }


class MatchedAlertConsumer:
    def __init__(self, idempotency_gate=None, suppression_auditor=None):
        self._running = False
        self._consumer = None
        self._idempotency_gate = idempotency_gate or IdempotencyGate()
        self._suppression_auditor = suppression_auditor or get_suppression_auditor()

    def _create_consumer(self):
        # Imported lazily so config/tests don't require a broker or librdkafka.
        from confluent_kafka import Consumer

        return Consumer(build_consumer_config())

    def start(self) -> None:
        self._consumer = self._create_consumer()
        self._consumer.subscribe([MATCHED_ALERTS_TOPIC])
        self._running = True

        signal.signal(signal.SIGINT, lambda *_: self.stop())
        signal.signal(signal.SIGTERM, lambda *_: self.stop())

        logger.info(
            "Consuming topic=%s group=%s (auto-commit OFF)",
            MATCHED_ALERTS_TOPIC,
            KAFKA_CONSUMER_GROUP,
        )
        self._report_gate_configuration()
        try:
            while self._running:
                msg = self._consumer.poll(KAFKA_POLL_TIMEOUT_SECONDS)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("Kafka error: %s", msg.error())
                    continue
                try:
                    self._handle(msg.value())
                except Exception:  # noqa: BLE001
                    # One bad message must never kill the loop. Without this,
                    # an exception escaping `_handle` exits the process, the
                    # uncommitted offset is redelivered to the restarted pod,
                    # and the same message poisons it again — a crashloop that
                    # stalls the whole partition.
                    logger.exception(
                        "Unhandled error processing a matched message; skipping it"
                    )
        finally:
            self._consumer.close()
            logger.info("Consumer closed")

    def _handle(self, raw: bytes) -> None:
        try:
            message = MatchedAlertMessage.from_bytes(raw)
        except Exception:
            deserialize_errors.inc()
            logger.exception("Failed to deserialize matched message")
            return

        messages_consumed.inc()
        decision = self._idempotency_gate.claim(message)

        if decision.outcome is IdempotencyOutcome.DUPLICATE:
            # A previous delivery of this same event reached a confirmed submit.
            # Nothing is sent to the API; the suppression is audited so it is
            # visible rather than silent (spec §7.5). C11 commits the offset here.
            try:
                self._suppression_auditor.record_suppression(
                    tenant_id=message.tenant_id,
                    automation_id=message.automation_id,
                    history_id=message.history_id,
                    fingerprint=message.fingerprint,
                    reason=REASON_DUPLICATE,
                    gate_flags=decision.gate_flags,
                )
            except Exception:  # noqa: BLE001
                # D17 replaces the stub with an HTTP client that will raise on
                # a 503. A failed audit must not fail the message: the run
                # already happened, and re-submitting it is the worse outcome.
                logger.exception(
                    "automations: could not audit a suppressed duplicate "
                    "tenant_id=%s automation_id=%s history_id=%s",
                    message.tenant_id,
                    message.automation_id,
                    message.history_id,
                )
            return

        # Claimed, ambiguous, or fail-open — all three submit (C11). Until the
        # submit call exists the key is left at `pending`, which is exactly what
        # a crash between claim and submit would leave: a redelivery proceeds.
        logger.info(
            "Consumed matched message tenant_id=%s automation_id=%s history_id=%s "
            "matched_m=%s idempotency=%s (submit lands in C11; offset not committed)",
            message.tenant_id,
            message.automation_id,
            message.history_id,
            message.matched_m,
            decision.outcome.value,
        )

    def _report_gate_configuration(self) -> None:
        """Say once, at startup, whether the gates are configured at all.

        An unset `REDIS_URL` is silent otherwise: every message fails open,
        `redis_errors` never increments (nothing is attempted), and the pod
        looks healthy while running 100% ungated. Same signal shape as
        keep-event-handler's `automation_index_config_missing`.
        """
        if redis_configured():
            gates_config_missing.labels(setting="redis_url").set(0)
            return
        gates_config_missing.labels(setting="redis_url").set(1)
        logger.warning(
            "automations: REDIS_URL is not set — the idempotency gate is "
            "disabled and every message will be submitted ungated (fail-open). "
            "Duplicate protection falls entirely to the API's unique "
            "(history_id, automation_id) constraint."
        )

    def stop(self) -> None:
        self._running = False
