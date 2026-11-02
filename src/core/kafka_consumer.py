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

Revoke drains: before a revoked partition's offsets are committed, its in-flight
workers are woken (backoff sleeps end) and waited for, bounded by
`IN_FLIGHT_DRAIN_TIMEOUT_SECONDS`. Without this the next owner re-reads a message
while the old worker still holds its provisional cooldown claim; the redelivery
would lose NX to its own sibling, be audited as a cooldown suppression and
committed — and if the old submit then failed, the automation never ran.
"""
from __future__ import annotations

import logging
import signal
import threading
import time
from typing import TYPE_CHECKING, Callable, Iterable, Optional

from src.bl.pipeline import SubmitPipeline
from src.config.consts import (
    IN_FLIGHT_DRAIN_TIMEOUT_SECONDS,
    WORKER_POOL_SIZE,
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
    revoke_drain_timeouts,
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


# Saturation ends when in-flight work drops to this (90% of the pool).
RESUME_BELOW_IN_FLIGHT = WORKER_POOL_SIZE - max(1, WORKER_POOL_SIZE // 10)


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
        # Per-partition in-flight work, so a revoke can drain it, and a cancel
        # event per partition epoch that wakes its workers' backoff sleeps.
        self._inflight: dict[PartitionKey, int] = {}
        self._inflight_cv = threading.Condition()
        self._cancel_events: dict[PartitionKey, tuple[int, threading.Event]] = {}
        # Partitions inside `_on_revoke`: their workers must stop retrying even
        # though the tracker still owns them until the revoke commit.
        self._revoking: set[PartitionKey] = set()

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

            # Flags only: a handler that set a worker's cancel Event could
            # deadlock against that Event's own lock if the signal landed while
            # this thread was inside `_cancel`. The loop exits within one poll
            # timeout and `finally` wakes the workers.
            signal.signal(signal.SIGINT, lambda *_: self._request_stop())
            signal.signal(signal.SIGTERM, lambda *_: self._request_stop())

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
            self._cancel_all()
            worker_pool_saturated.set(0)
            partitions_paused_uncommitted.set(0)
            self._worker_pool.shutdown(wait=True)
            if consumer is not None:
                self._commit_settled(consumer, synchronous=True)
                consumer.close()
                logger.info("Consumer closed")

    def stop(self) -> None:
        """Stop consuming and wake workers sleeping in a retry backoff.

        Not for signal handlers (see `start`): those use `_request_stop`.
        """
        self._request_stop()
        self._cancel_all()

    def _request_stop(self) -> None:
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
        cancelled = self._cancel_event(key, epoch)
        with self._inflight_cv:
            self._inflight[key] = self._inflight.get(key, 0) + 1
        if not self._worker_pool.try_submit(
            self._handle_safely, msg.value(), key, offset, epoch, cancelled
        ):
            self._finish_inflight(key)
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
        self,
        raw: bytes,
        key: PartitionKey,
        offset: int,
        epoch: int,
        cancelled: threading.Event,
    ) -> None:
        settled = True
        try:
            settled = self._handle(
                raw,
                cancelled=cancelled,
                still_owned=lambda: (
                    key not in self._revoking and self._tracker.owns(key, epoch)
                ),
            )
        except Exception:  # noqa: BLE001
            # One bad message must not kill a worker or wedge its partition:
            # commits are contiguous, so an unsettled poison message would block
            # every later offset forever. Skipped and counted instead.
            handle_errors.inc()
            logger.exception("Unhandled error processing a matched message; skipping it")
        finally:
            # Settle before leaving the in-flight count, so a revoke drain that
            # wakes on the count sees this offset settled and commits it.
            if settled:
                self._tracker.settle(key, offset, epoch)
            self._finish_inflight(key)

    def _handle(
        self,
        raw: bytes,
        *,
        cancelled: Optional[threading.Event] = None,
        still_owned: Callable[[], bool] = lambda: True,
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
            message,
            cancelled=cancelled if cancelled is not None else self._stopping,
            still_owned=still_owned,
        )
        return result is not None

    # -- in-flight work per partition ---------------------------------------------

    def _cancel_event(self, key: PartitionKey, epoch: int) -> threading.Event:
        current = self._cancel_events.get(key)
        if current is None or current[0] != epoch:
            current = (epoch, threading.Event())
            self._cancel_events[key] = current
        return current[1]

    def _cancel(self, keys: Iterable[PartitionKey]) -> None:
        for key in keys:
            current = self._cancel_events.pop(key, None)
            if current is not None:
                current[1].set()

    def _cancel_all(self) -> None:
        self._cancel(list(self._cancel_events))

    def _finish_inflight(self, key: PartitionKey) -> None:
        with self._inflight_cv:
            remaining = self._inflight.get(key, 0) - 1
            if remaining > 0:
                self._inflight[key] = remaining
            else:
                self._inflight.pop(key, None)
            self._inflight_cv.notify_all()

    def _drain(self, keys: list[PartitionKey]) -> None:
        """Wait for in-flight work on `keys`, bounded.

        Workers backing off have already been woken (`_cancel`); what remains
        is at most one HTTP attempt each, bounded by the submit timeouts. The
        poll thread blocks here inside the rebalance callback — heartbeats run
        on librdkafka's own thread, and the bound sits far below
        `max.poll.interval.ms`.
        """
        deadline = time.monotonic() + IN_FLIGHT_DRAIN_TIMEOUT_SECONDS
        with self._inflight_cv:
            while any(self._inflight.get(key, 0) for key in keys):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    revoke_drain_timeouts.inc()
                    logger.warning(
                        "Revoke drain timed out after %.0fs with work still in "
                        "flight; its messages will be redelivered to the next owner",
                        IN_FLIGHT_DRAIN_TIMEOUT_SECONDS,
                    )
                    return
                self._inflight_cv.wait(remaining)

    # -- backpressure -----------------------------------------------------------

    def _apply_backpressure(self, consumer: Consumer) -> None:
        assigned = {_key(tp): tp for tp in consumer.assignment()}
        saturated = not self._worker_pool.has_capacity()
        if self._saturated and not saturated:
            # Hysteresis: resume only once a tenth of the pool is free. Resuming
            # on the first free slot would refill the pool at once and re-pause
            # every iteration; each pause discards librdkafka's prefetched
            # data, so the flip-flop refetches up to a fetch size per partition
            # several times a second.
            with self._inflight_cv:
                in_flight = sum(self._inflight.values())
            saturated = in_flight > RESUME_BELOW_IN_FLIGHT
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
            result = consumer.commit(offsets=offsets, asynchronous=not synchronous)
        except KafkaException as exc:
            offset_commit_errors.inc(len(due))
            logger.warning(
                "Offset commit failed (%s); retrying on the next tick: %s",
                "sync" if synchronous else "async",
                exc,
            )
            return
        committed = due
        if synchronous and result:
            # A synchronous commit raises only for a request-level error;
            # per-partition failures come back on the returned list.
            failed = {_key(tp) for tp in result if tp.error is not None}
            if failed:
                offset_commit_errors.inc(len(failed))
                logger.warning(
                    "Offset commit failed for %s partition(s) (sync)", len(failed)
                )
                committed = [(key, position) for key, position in due if key not in failed]
        self._tracker.mark_committed(committed)
        offsets_committed.inc(len(committed))

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
        # Stop retries, drain in-flight work, then commit what settled — all
        # while this consumer still owns the partitions. The next owner cannot
        # receive them until this callback returns, so no message is ever
        # processed by two deliveries at once. Anything unsettled (abandoned,
        # or still running past the drain bound) is redelivered to the new
        # owner, and a late settle from the old epoch is ignored.
        keys = [_key(tp) for tp in partitions]
        self._revoking.update(keys)
        self._cancel(keys)
        self._drain(keys)
        self._commit_settled(consumer, synchronous=True, keys=keys)
        self._forget(keys)
        logger.info("Revoked %s partition(s)", len(partitions))

    def _on_lost(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Ownership is already gone: committing now could overwrite the new
        # owner's progress. Drop state only.
        keys = [_key(tp) for tp in partitions]
        self._cancel(keys)
        self._forget(keys)
        logger.warning("Lost %s partition(s) without a clean revoke", len(partitions))

    def _forget(self, keys: list[PartitionKey]) -> None:
        self._tracker.revoke(keys)
        self._paused.difference_update(keys)
        self._window_paused.difference_update(keys)
        self._revoking.difference_update(keys)
        self._reconcile_all = True
