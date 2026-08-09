"""Matched-alerts Kafka consumer (C8 skeleton).

Consumes the matched-alerts topic with **auto-commit off** and logs each message.
Gates (C9/C10), submit + offset commit + fail-open (C11) are intentionally absent
— the loop never commits offsets yet.
"""
import logging
import signal

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
    def __init__(self):
        self._running = False
        self._consumer = None

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
        # Skeleton: log only. No gates, no submit, offset NOT committed (C9/C10/C11).
        logger.info(
            "Consumed matched message tenant_id=%s automation_id=%s history_id=%s matched_m=%s "
            "(skeleton: no gate/submit; offset not committed)",
            message.tenant_id,
            message.automation_id,
            message.history_id,
            message.matched_m,
        )

    def stop(self) -> None:
        self._running = False
