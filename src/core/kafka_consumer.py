"""Matched-alerts Kafka consumer.

Consumes the matched-alerts topic with **auto-commit off**. Each message runs
the C11 submit pipeline (gates → submit → gate lifecycle) in a bounded worker
pool, and its offset is committed only once the pipeline settles it — an API
acknowledgement, an audited suppression, or a definitive refusal. A message that
is still retrying, or was handed back on shutdown/revoke, is never committed
past, so Kafka redelivers it.

Threading: only the poll thread touches the Kafka client (poll, pause/resume,
seek, commit). Workers report settled offsets to an `OffsetTracker`, and the
poll thread commits each partition's contiguous position on a short interval,
plus synchronously on revoke and at shutdown. The poll thread performs no Redis
or HTTP I/O, so polling cadence (heartbeats, `max.poll.interval.ms`) is
independent of how long submits or their retries take (ADR-007).

Backpressure: when every worker is busy, all assigned partitions pause; when one
partition's uncommitted window is full (a stuck message with many settled ones
behind it), only that partition pauses. Polling continues throughout.
"""
from __future__ import annotations

import logging
import signal
import threading
import time
from typing import TYPE_CHECKING, Callable, Iterable, Optional

from src.bl.pipeline import SubmitPipeline
from src.config.consts import (
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_BACKPRESSURE_POLL_TIMEOUT_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_COMMIT_INTERVAL_SECONDS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_POLL_TIMEOUT_SECONDS,
    MATCHED_ALERTS_TOPIC,
    MAX_UNCOMMITTED_OFFSETS_PER_PARTITION,
)
from src.core.metrics import (
    deserialize_errors,
    handle_errors,
    messages_consumed,
    messages_rewound,
    offset_commit_errors,
    offsets_committed,
    partitions_paused_uncommitted,
    worker_pool_saturated,
    worker_pool_saturation_events,
)
from src.core.offset_tracker import OffsetTracker, PartitionKey
from src.core.worker_pool import WorkerPool
from src.models.matched_message import MatchedAlertMessage

if TYPE_CHECKING:
    from confluent_kafka import Consumer, TopicPartition

logger = logging.getLogger(__name__)


def build_consumer_config() -> dict[str, object]:
    """confluent-kafka consumer config.

    `enable.auto.commit=False` is the contract: offsets are committed only for
    settled messages (C11), never automatically.
    """
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "enable.auto.commit": False,
        "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
    }


def _key(partition: TopicPartition) -> PartitionKey:
    return (partition.topic, partition.partition)


class MatchedAlertConsumer:
    def __init__(
        self,
        pipeline: SubmitPipeline,
        worker_pool: WorkerPool,
        offset_tracker: Optional[OffsetTracker] = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._running = False
        self._stopping = threading.Event()
        self._consumer: Optional[Consumer] = None
        self._pipeline = pipeline
        self._worker_pool = worker_pool
        self._tracker = offset_tracker if offset_tracker is not None else OffsetTracker()
        self._clock = monotonic_clock
        self._next_commit_at = 0.0
        # Poll-thread state: partitions we paused, and why.
        self._paused: set[PartitionKey] = set()
        self._window_paused: set[PartitionKey] = set()
        self._saturated = False
        # Set by rebalance callbacks: re-apply pause/resume to every assigned
        # partition, since a fresh assignment's pause state is not ours.
        self._reconcile_all = True

    def _create_consumer(self) -> Consumer:
        # Imported lazily so config/tests don't require a broker or librdkafka.
        from confluent_kafka import Consumer

        return Consumer({**build_consumer_config(), "on_commit": self._on_commit})

    def start(self) -> None:
        consumer: Optional[Consumer] = None
        try:
            consumer = self._create_consumer()
            self._consumer = consumer
            consumer.subscribe(
                [MATCHED_ALERTS_TOPIC],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
                on_lost=self._on_lost,
            )
            self._running = True

            signal.signal(signal.SIGINT, lambda *_: self.stop())
            signal.signal(signal.SIGTERM, lambda *_: self.stop())

            logger.info(
                "Consuming topic=%s group=%s (auto-commit OFF)",
                MATCHED_ALERTS_TOPIC,
                KAFKA_CONSUMER_GROUP,
            )
            while self._running:
                self._apply_backpressure(consumer)
                # Short poll while anything is paused, so freed capacity or a
                # drained window resumes partitions promptly. `poll` keeps
                # serving callbacks, heartbeats and rebalances either way.
                msg = consumer.poll(
                    KAFKA_BACKPRESSURE_POLL_TIMEOUT_SECONDS
                    if self._paused
                    else KAFKA_POLL_TIMEOUT_SECONDS
                )
                self._commit_settled(consumer, synchronous=False)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("Kafka error: %s", msg.error())
                    continue
                self._dispatch(consumer, msg)
        finally:
            # Workers retrying a submit abandon at their next check; workers
            # mid-request finish it (bounded by the HTTP timeout). Draining
            # them first means the final commit sees every settled offset, and
            # no commit ever runs through a closed client.
            self._stopping.set()
            worker_pool_saturated.set(0)
            partitions_paused_uncommitted.set(0)
            self._worker_pool.shutdown(wait=True)
            if consumer is not None:
                self._commit_settled(consumer, synchronous=True)
                consumer.close()
                logger.info("Consumer closed")

    def stop(self) -> None:
        self._running = False
        self._stopping.set()

    # -- dispatch ---------------------------------------------------------------

    def _dispatch(self, consumer: Consumer, msg) -> None:
        key = (msg.topic(), msg.partition())
        offset = msg.offset()
        if key in self._paused or not self._worker_pool.has_capacity():
            self._rewind(consumer, key, offset)
            return
        epoch = self._tracker.track(key, offset)
        if not self._worker_pool.try_submit(
            self._handle_safely, msg.value(), key, offset, epoch
        ):
            # Only this poll thread submits work, so capacity cannot be
            # consumed between `has_capacity` and `try_submit`. Failing here
            # indicates a pool lifecycle bug. Exit with the offset uncommitted
            # (it is tracked as outstanding) rather than drop the message.
            raise RuntimeError("worker pool rejected an admitted message")

    def _rewind(self, consumer: Consumer, key: PartitionKey, offset: int) -> None:
        """Hand a polled-but-undispatchable message back to the fetcher.

        Kafka advanced this partition's position past the message when `poll`
        returned it, so without a seek it would be skipped until the next
        restart. Tracking it as outstanding first keeps every commit at or
        below it even if the seek fails.
        """
        from confluent_kafka import TopicPartition

        self._tracker.track(key, offset)
        messages_rewound.inc()
        consumer.seek(TopicPartition(key[0], key[1], offset))

    def _handle_safely(
        self, raw: bytes, key: PartitionKey, offset: int, epoch: int
    ) -> None:
        settled = True
        try:
            settled = self._handle(
                raw, still_owned=lambda: self._tracker.owns(key, epoch)
            )
        except Exception:  # noqa: BLE001
            # One bad message must not kill a worker or wedge its partition:
            # commits are contiguous, so an unsettled poison message would block
            # every later offset forever. Skipped and counted instead.
            handle_errors.inc()
            logger.exception("Unhandled error processing a matched message; skipping it")
        if settled:
            self._tracker.settle(key, offset, epoch)

    def _handle(
        self, raw: bytes, *, still_owned: Callable[[], bool] = lambda: True
    ) -> bool:
        """Process one message. True when settled (its offset may commit)."""
        try:
            message = MatchedAlertMessage.from_bytes(raw)
        except Exception:
            # Poison: no redelivery can make it parse. Settled (skipped) so it
            # does not block its partition.
            deserialize_errors.inc()
            logger.exception("Failed to deserialize matched message; skipping it")
            return True

        messages_consumed.inc()
        result = self._pipeline.run(
            message, cancelled=self._stopping, still_owned=still_owned
        )
        return result is not None

    # -- backpressure -----------------------------------------------------------

    def _apply_backpressure(self, consumer: Consumer) -> None:
        assigned = {_key(tp): tp for tp in consumer.assignment()}
        saturated = not self._worker_pool.has_capacity()
        if saturated != self._saturated:
            self._report_saturation(saturated)
            self._saturated = saturated

        full = self._tracker.windows_at_least(MAX_UNCOMMITTED_OFFSETS_PER_PARTITION)
        window_paused = full & assigned.keys()
        if window_paused != self._window_paused:
            self._report_window_pauses(window_paused)
            self._window_paused = window_paused
        partitions_paused_uncommitted.set(len(window_paused))

        desired = set(assigned) if saturated else window_paused
        if self._reconcile_all:
            to_pause, to_resume = desired, set(assigned) - desired
            self._reconcile_all = False
        else:
            to_pause = desired - self._paused
            to_resume = (self._paused - desired) & assigned.keys()
        if to_pause:
            consumer.pause([assigned[key] for key in to_pause])
        if to_resume:
            consumer.resume([assigned[key] for key in to_resume])
        self._paused = desired

    @staticmethod
    def _report_saturation(saturated: bool) -> None:
        if saturated:
            worker_pool_saturated.set(1)
            worker_pool_saturation_events.inc()
            logger.warning(
                "Worker pool saturated; Kafka partitions paused while polling continues"
            )
        else:
            worker_pool_saturated.set(0)
            logger.info("Worker capacity available; Kafka partitions resumed")

    def _report_window_pauses(self, window_paused: set[PartitionKey]) -> None:
        for topic, partition in sorted(window_paused - self._window_paused):
            logger.warning(
                "Partition %s[%s] paused: %s+ settled offsets wait behind a message "
                "that is still retrying",
                topic,
                partition,
                MAX_UNCOMMITTED_OFFSETS_PER_PARTITION,
            )
        for topic, partition in sorted(self._window_paused - window_paused):
            logger.info("Partition %s[%s] resumed: uncommitted window drained", topic, partition)

    # -- commits ----------------------------------------------------------------

    def _commit_settled(
        self,
        consumer: Consumer,
        *,
        synchronous: bool,
        keys: Optional[Iterable[PartitionKey]] = None,
    ) -> None:
        """Commit each partition's contiguous settled position, if it moved.

        Asynchronous commits are rate-limited to `KAFKA_COMMIT_INTERVAL_SECONDS`
        and confirmed by `_on_commit`; synchronous ones (revoke, shutdown) always
        run. A failed commit is offered again on the next tick — re-committing
        a position is harmless, and redelivery after a lost commit is absorbed
        by the idempotent pipeline.
        """
        from confluent_kafka import KafkaException, TopicPartition

        if not synchronous:
            now = self._clock()
            if now < self._next_commit_at:
                return
            self._next_commit_at = now + KAFKA_COMMIT_INTERVAL_SECONDS

        due = self._tracker.commits_due(keys)
        if not due:
            return
        offsets = [
            TopicPartition(topic, partition, position)
            for (topic, partition), position in due
        ]
        try:
            consumer.commit(offsets=offsets, asynchronous=not synchronous)
        except KafkaException as exc:
            offset_commit_errors.inc(len(due))
            logger.warning(
                "Offset commit failed (%s); retrying on the next tick: %s",
                "sync" if synchronous else "async",
                exc,
            )
            return
        self._tracker.mark_committed(due)
        offsets_committed.inc(len(due))

    def _on_commit(self, err, partitions: list[TopicPartition]) -> None:
        failed = (
            list(partitions)
            if err is not None
            else [tp for tp in partitions if tp.error is not None]
        )
        if not failed:
            return
        offset_commit_errors.inc(len(failed))
        self._tracker.mark_commit_failed(_key(tp) for tp in failed)
        logger.warning(
            "Async offset commit failed for %s partition(s); retrying on the next "
            "tick: %s",
            len(failed),
            err or failed[0].error,
        )

    # -- rebalance (all run on the poll thread, inside `poll`) -------------------

    def _on_assign(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        self._tracker.assign(_key(tp) for tp in partitions)
        self._reconcile_all = True
        logger.info("Assigned %s partition(s)", len(partitions))

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Commit what is settled while this consumer still owns the partitions;
        # anything unsettled is redelivered to the new owner. Workers still
        # holding revoked messages stop retrying (`still_owned` turns False)
        # and their late settles are ignored (epoch mismatch).
        keys = [_key(tp) for tp in partitions]
        self._commit_settled(consumer, synchronous=True, keys=keys)
        self._forget(keys)
        logger.info("Revoked %s partition(s)", len(partitions))

    def _on_lost(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Ownership is already gone: committing now could overwrite the new
        # owner's progress. Drop state only.
        keys = [_key(tp) for tp in partitions]
        self._forget(keys)
        logger.warning("Lost %s partition(s) without a clean revoke", len(partitions))

    def _forget(self, keys: list[PartitionKey]) -> None:
        self._tracker.revoke(keys)
        self._paused.difference_update(keys)
        self._window_paused.difference_update(keys)
        self._reconcile_all = True
