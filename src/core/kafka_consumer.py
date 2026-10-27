"""Matched-alerts Kafka consumer.

Consumes the matched-alerts topic with **auto-commit off**, runs the idempotency
gate (C9) and logs the decision. Cooldown (C10) and submit + offset commit +
fail-open (C11) are still absent — the loop never commits offsets yet.

Per-message work runs in a bounded worker pool. The Kafka poll thread performs
no Redis or downstream I/O; this preserves polling cadence during gate outages
without creating an unbounded executor queue.
"""
from __future__ import annotations

import logging
import signal
from typing import TYPE_CHECKING, Optional

from src.bl.gates.idempotency import IdempotencyGate
from src.bl.suppression_audit import REASON_DUPLICATE, SuppressionAuditor
from src.config.consts import (
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_BACKPRESSURE_POLL_TIMEOUT_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_POLL_TIMEOUT_SECONDS,
    MATCHED_ALERTS_TOPIC,
)
from src.core.metrics import (
    deserialize_errors,
    handle_errors,
    messages_consumed,
    worker_pool_saturated,
    worker_pool_saturation_events,
)
from src.core.worker_pool import WorkerPool
from src.models.idempotency import IdempotencyOutcome
from src.models.matched_message import MatchedAlertMessage

if TYPE_CHECKING:
    from confluent_kafka import Consumer

logger = logging.getLogger(__name__)


def build_consumer_config() -> dict[str, object]:
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
    def __init__(
        self,
        idempotency_gate: IdempotencyGate,
        suppression_auditor: SuppressionAuditor,
        worker_pool: WorkerPool,
    ) -> None:
        self._running = False
        self._consumer: Optional[Consumer] = None
        self._idempotency_gate = idempotency_gate
        self._suppression_auditor = suppression_auditor
        self._worker_pool = worker_pool

    def _create_consumer(self) -> Consumer:
        # Imported lazily so config/tests don't require a broker or librdkafka.
        from confluent_kafka import Consumer

        return Consumer(build_consumer_config())

    def start(self) -> None:
        consumer: Optional[Consumer] = None
        backpressure_active = False
        try:
            consumer = self._create_consumer()
            self._consumer = consumer
            consumer.subscribe([MATCHED_ALERTS_TOPIC])
            self._running = True

            signal.signal(signal.SIGINT, lambda *_: self.stop())
            signal.signal(signal.SIGTERM, lambda *_: self.stop())

            logger.info(
                "Consuming topic=%s group=%s (auto-commit OFF)",
                MATCHED_ALERTS_TOPIC,
                KAFKA_CONSUMER_GROUP,
            )
            while self._running:
                if not self._worker_pool.has_capacity():
                    self._pause_assigned_partitions(consumer)
                    if not backpressure_active:
                        self._report_backpressure_started()
                        backpressure_active = True
                    # `poll` remains active for callbacks, heartbeats, and
                    # rebalance progress while assigned partitions are paused.
                    paused_message = consumer.poll(
                        KAFKA_BACKPRESSURE_POLL_TIMEOUT_SECONDS
                    )
                    if paused_message is not None and paused_message.error():
                        logger.error("Kafka error: %s", paused_message.error())
                    elif paused_message is not None:
                        raise RuntimeError(
                            "Kafka returned a message while assigned partitions were paused"
                        )
                    continue

                if backpressure_active:
                    self._resume_after_backpressure(consumer)
                    backpressure_active = False

                msg = consumer.poll(KAFKA_POLL_TIMEOUT_SECONDS)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("Kafka error: %s", msg.error())
                    continue
                if not self._worker_pool.try_submit(self._handle_safely, msg.value()):
                    # Only this poll thread submits work, so capacity cannot be
                    # consumed between `has_capacity` and `try_submit`. Failing
                    # here indicates a pool lifecycle bug. Exit with the Kafka
                    # offset uncommitted rather than drop the message.
                    raise RuntimeError("worker pool rejected an admitted message")
        finally:
            worker_pool_saturated.set(0)
            # Future C11 workers commit offsets. Drain them before closing the
            # Kafka client so shutdown never commits through a closed handle.
            self._worker_pool.shutdown(wait=True)
            if consumer is not None:
                consumer.close()
                logger.info("Consumer closed")

    @staticmethod
    def _pause_assigned_partitions(consumer: Consumer) -> None:
        partitions = consumer.assignment()
        if partitions:
            consumer.pause(partitions)

    @staticmethod
    def _report_backpressure_started() -> None:
        worker_pool_saturated.set(1)
        worker_pool_saturation_events.inc()
        logger.warning(
            "Worker pool saturated; Kafka partitions paused while polling continues"
        )

    @staticmethod
    def _resume_after_backpressure(consumer: Consumer) -> None:
        partitions = consumer.assignment()
        if partitions:
            consumer.resume(partitions)
        worker_pool_saturated.set(0)
        logger.info("Worker capacity available; Kafka partitions resumed")

    def _handle_safely(self, raw: bytes) -> None:
        try:
            self._handle(raw)
        except Exception:  # noqa: BLE001
            # One bad message must not kill a worker or poison its partition.
            handle_errors.inc()
            logger.exception("Unhandled error processing a matched message; skipping it")

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

    def stop(self) -> None:
        self._running = False
