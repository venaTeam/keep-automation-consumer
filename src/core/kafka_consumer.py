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
from src.core.metrics import deserialize_errors, messages_consumed
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
        try:
            while self._running:
                msg = self._consumer.poll(KAFKA_POLL_TIMEOUT_SECONDS)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("Kafka error: %s", msg.error())
                    continue
                self._handle(msg.value())
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
            self._suppression_auditor.record_suppression(
                tenant_id=message.tenant_id,
                automation_id=message.automation_id,
                history_id=message.history_id,
                fingerprint=message.fingerprint,
                reason=REASON_DUPLICATE,
                gate_flags=decision.gate_flags,
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

    def stop(self) -> None:
        self._running = False
