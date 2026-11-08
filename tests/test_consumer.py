"""Consumer tests: config, message parsing, and the C11 poll loop (dispatch,
backpressure, contiguous commits, rebalance, shutdown)."""
import json
import logging
import signal
import threading
import time

import pytest
from confluent_kafka import KafkaException, TopicPartition
from prometheus_client import REGISTRY

import src.core.kafka_consumer as kafka_consumer_module
from src.core.kafka_consumer import build_consumer_config
from src.core.offset_tracker import OffsetTracker
from src.core.worker_pool import BoundedWorkerPool
from src.models.matched_message import MatchedAlertMessage
from src.models.submit import SubmitOutcome
from tests.helpers import (
    ACCEPTED,
    InMemoryRedis,
    ScriptedSubmitClient,
    build_test_consumer,
    build_test_pipeline,
    failure,
)

# B5 matched-message shape: the event handler publishes alert.id.
CANONICAL_MESSAGE = {
    "tenant_id": "keep",
    "alert": {
        "application": "payments",
        "severity": "critical",
        "status": "firing",
        "component": "api",
        "site": "us-east-1",
        "environment": "prod",
        "operator": "team-payments",
        "fingerprint": "abc123",
        "id": "evt-789",
        "time_created": "2026-07-12T14:03:00Z",
    },
    # The doc elides the tail of the uuid ("3f2b..."); spelled out here.
    "automation_id": "3f2b8c1e-4d6a-4b2f-9e77-0a1b2c3d4e5f",
    "matched_m": 2,
    "cooldown": {"fields": ["node_name", "site"], "seconds": 300, "scheme_ver": 1},
}


def _encode(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def _without(key: str) -> dict:
    return {k: v for k, v in CANONICAL_MESSAGE.items() if k != key}


def _counter(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_auto_commit_is_off():
    cfg = build_consumer_config()
    assert cfg["enable.auto.commit"] is False


def test_consumer_config_has_group_and_servers():
    cfg = build_consumer_config()
    assert cfg["group.id"]
    assert cfg["bootstrap.servers"]


def test_matched_message_from_bytes():
    msg = MatchedAlertMessage.from_bytes(_encode(CANONICAL_MESSAGE))

    assert msg.tenant_id == "keep"
    assert msg.automation_id == "3f2b8c1e-4d6a-4b2f-9e77-0a1b2c3d4e5f"
    assert msg.matched_m == 2
    assert msg.history_id == "evt-789"
    assert msg.fingerprint == "abc123"
    assert msg.cooldown["seconds"] == 300
    assert msg.alert == CANONICAL_MESSAGE["alert"]


@pytest.mark.parametrize(
    "identity, expected",
    [
        ({"history_id": "legacy-event"}, None),
        ({"id": "b5-event", "history_id": "legacy-event"}, "b5-event"),
        ({"id": None, "history_id": "legacy-event"}, None),
        ({"id": "", "history_id": "legacy-event"}, ""),
        ({}, None),
    ],
)
def test_event_identity_compatibility_preserves_alert(identity, expected):
    alert = {k: v for k, v in CANONICAL_MESSAGE["alert"].items() if k != "id"}
    alert.update(identity)
    msg = MatchedAlertMessage.from_bytes(
        _encode({**CANONICAL_MESSAGE, "alert": alert})
    )

    assert msg.history_id == expected
    assert msg.alert == alert


def test_matched_message_requires_tenant_id():
    """A tenant-less message must fail loudly — never default to a tenant."""
    with pytest.raises(KeyError) as excinfo:
        MatchedAlertMessage.from_bytes(_encode(_without("tenant_id")))

    assert excinfo.value.args[0] == "tenant_id"


def test_tenantless_message_is_counted_as_a_deserialize_error():
    """The loop drops the message and counts it, rather than consuming it untenanted."""
    errors_before = _counter("keep_automation_consumer_deserialize_errors_total")
    consumed_before = _counter("keep_automation_consumer_messages_consumed_total")

    settled = build_test_consumer()._handle(_encode(_without("tenant_id")))

    assert settled is True  # poison: skipped, never blocks its partition

    assert (
        _counter("keep_automation_consumer_deserialize_errors_total")
        == errors_before + 1
    )
    assert (
        _counter("keep_automation_consumer_messages_consumed_total") == consumed_before
    )




def test_canonical_message_runs_both_gates_and_submits():
    """C11 wires C10: a message with cooldown config claims, submits and arms."""
    redis = InMemoryRedis()
    client = ScriptedSubmitClient()
    consumer = build_test_consumer(
        pipeline=build_test_pipeline(redis=redis, submit_client=client)
    )

    settled = consumer._handle(_encode(CANONICAL_MESSAGE))

    assert settled is True
    (request,) = client.requests
    assert request.tenant_id == "keep"
    assert request.gate_flags["idempotency"] == "claimed"
    # The canonical alert has no node_name, so the cooldown runs + warns.
    assert request.gate_flags["cooldown_reason"] == "missing_field"


# -- poll loop ------------------------------------------------------------------
#
# Only the poll thread touches Kafka; workers settle offsets in a tracker and the
# poll thread commits each partition's contiguous position. These tests drive
# `start()` against a scripted fake client.

TOPIC = "matched-alerts"


class _FakeMessage:
    def __init__(self, value=b"{}", *, partition=0, offset=0, error=None):
        self._value = value
        self._partition = partition
        self._offset = offset
        self._error = error

    def error(self):
        return self._error

    def value(self):
        return self._value

    def topic(self):
        return TOPIC

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class _FakeKafkaConsumer:
    """Yields scripted poll() results, then stops the loop.

    A script item may be a message, None, or a callable run in place of a poll
    (to trigger a rebalance, finish deferred work, advance a clock...). Like
    librdkafka, messages of paused partitions are held back unless
    `ignore_pause` is set.
    """

    def __init__(self, script, on_exhausted, partitions=(0,), ignore_pause=False):
        self._script = list(script)
        self._on_exhausted = on_exhausted
        self._assignment = [TopicPartition(TOPIC, p) for p in partitions]
        self.ignore_pause = ignore_pause
        self.callbacks = {}
        self.subscribed = None
        self.paused = set()
        self.pause_calls = []
        self.resume_calls = []
        self.seeks = []
        self.commits = []
        self.closed = False
        self.polls = 0

    def subscribe(self, topics, on_assign=None, on_revoke=None, on_lost=None):
        self.subscribed = topics
        self.callbacks = {"assign": on_assign, "revoke": on_revoke, "lost": on_lost}

    def poll(self, _timeout):
        self.polls += 1
        if self.polls > 20_000:
            # A message held back by a pause that never lifts would otherwise
            # spin here forever; fail the test instead of hanging the suite.
            raise AssertionError("poll budget exhausted: a partition never resumed")
        if not self._script:
            self._on_exhausted()
            return None
        item = self._script[0]
        if (
            isinstance(item, _FakeMessage)
            and item.error() is None
            and item.partition() in self.paused
            and not self.ignore_pause
        ):
            return None
        self._script.pop(0)
        if callable(item):
            item(self)
            return None
        return item

    def assignment(self):
        return list(self._assignment)

    def pause(self, partitions):
        keys = {tp.partition for tp in partitions}
        self.pause_calls.append(keys)
        self.paused |= keys

    def resume(self, partitions):
        keys = {tp.partition for tp in partitions}
        self.resume_calls.append(keys)
        self.paused -= keys

    def seek(self, partition):
        self.seeks.append((partition.partition, partition.offset))

    def commit(self, offsets, asynchronous):
        self.commits.append(({tp.partition: tp.offset for tp in offsets}, asynchronous))

    def committed_position(self, partition):
        positions = [c[partition] for c, _ in self.commits if partition in c]
        return positions[-1] if positions else None

    def revoke(self, *partitions):
        tps = [TopicPartition(TOPIC, p) for p in partitions]
        self.callbacks["revoke"](self, tps)
        self._assignment = [
            tp for tp in self._assignment if tp.partition not in partitions
        ]

    def lose(self, *partitions):
        tps = [TopicPartition(TOPIC, p) for p in partitions]
        self.callbacks["lost"](self, tps)
        self._assignment = [
            tp for tp in self._assignment if tp.partition not in partitions
        ]

    def assign(self, *partitions):
        tps = [TopicPartition(TOPIC, p) for p in partitions]
        self._assignment.extend(tps)
        self.callbacks["assign"](self, tps)

    def close(self):
        self.closed = True


class _DeferredWorkerPool:
    """Accepts work but runs it only when the test says so; unrun work is
    what a shutdown abandons."""

    def __init__(self, capacity=100):
        self.capacity = capacity
        self.tasks = []

    def has_capacity(self):
        return len([t for t in self.tasks if t is not None]) < self.capacity

    def try_submit(self, function, *args):
        self.tasks.append((function, args))
        return True

    def run(self, index):
        function, args = self.tasks[index]
        self.tasks[index] = None
        function(*args)

    def shutdown(self, *, wait):
        pass


def _run_loop(consumer, script, monkeypatch, **fake_kwargs):
    # `start()` installs process-global SIGINT/SIGTERM handlers. Left in place,
    # the pytest session's Ctrl-C would end up bound to a dead consumer's
    # stop().
    monkeypatch.setattr(signal, "signal", lambda *_: None)
    fake = _FakeKafkaConsumer(script, on_exhausted=consumer.stop, **fake_kwargs)
    consumer._create_consumer = lambda: fake
    consumer.start()
    return fake


def _accepting_consumer(**kwargs):
    return build_test_consumer(
        pipeline=build_test_pipeline(
            redis=InMemoryRedis(), submit_client=ScriptedSubmitClient()
        ),
        **kwargs,
    )


def _message(offset, partition=0):
    payload = json.loads(json.dumps(CANONICAL_MESSAGE))
    payload["alert"]["id"] = f"evt-{partition}-{offset}"
    payload["cooldown"] = None
    return _FakeMessage(_encode(payload), partition=partition, offset=offset)


def test_poll_loop_dispatches_message_processing_to_worker_pool(monkeypatch):
    pool = _DeferredWorkerPool()
    consumer = build_test_consumer(worker_pool=pool)

    _run_loop(consumer, [_FakeMessage(b"first", offset=7)], monkeypatch)

    (task,) = pool.tasks
    function, args = task
    assert function == consumer._handle_safely
    assert args[:3] == (b"first", (TOPIC, 0), 7)


def test_settled_offsets_are_committed_after_workers_drain(monkeypatch):
    events = []

    class OrderedWorkerPool:
        def has_capacity(self):
            return True

        def try_submit(self, function, *args):
            function(*args)
            return True

        def shutdown(self, *, wait):
            events.append("workers_drained")

    consumer = _accepting_consumer(
        worker_pool=OrderedWorkerPool(), monotonic_clock=lambda: 0.0
    )
    monkeypatch.setattr(signal, "signal", lambda *_: None)
    fake = _FakeKafkaConsumer([_message(5)], on_exhausted=consumer.stop)
    fake.commit = lambda offsets, asynchronous: events.append(
        ("commit", [(tp.partition, tp.offset) for tp in offsets], asynchronous)
    )
    fake.close = lambda: events.append("kafka_closed")
    consumer._create_consumer = lambda: fake

    consumer.start()

    # The final commit runs after workers drain and before the client closes,
    # synchronously, at one past the settled offset.
    assert events[-3:] == [
        "workers_drained",
        ("commit", [(0, 6)], False),
        "kafka_closed",
    ]


def test_commits_are_contiguous_per_partition(monkeypatch):
    """A message still in flight on p0 holds back p0 only; p1 commits."""
    pool = _DeferredWorkerPool()
    consumer = _accepting_consumer(worker_pool=pool)

    script = [
        _message(0, partition=0),  # stays unsettled (task 0)
        _message(1, partition=0),
        _message(2, partition=0),
        _message(0, partition=1),
        lambda fake: [pool.run(i) for i in (1, 2, 3)],
    ]
    fake = _run_loop(consumer, script, monkeypatch, partitions=(0, 1))

    assert fake.committed_position(1) == 1
    # Never past offset 0 on p0, although 1 and 2 settled.
    assert all(c.get(0, 0) == 0 for c, _ in fake.commits)


def test_out_of_order_completion_commits_once_the_gap_closes(monkeypatch):
    pool = _DeferredWorkerPool()
    clock = {"now": 0.0}
    consumer = _accepting_consumer(
        worker_pool=pool, monotonic_clock=lambda: clock["now"]
    )

    def finish(*indices):
        def step(_fake):
            for i in indices:
                pool.run(i)
            clock["now"] += 10.0  # past the commit interval

        return step

    script = [_message(0), _message(1), _message(2), finish(2, 1), None, finish(0), None]
    fake = _run_loop(consumer, script, monkeypatch)

    positions = [c[0] for c, asynchronous in fake.commits if asynchronous]
    assert positions == [0, 3]


def test_a_raising_handle_is_skipped_and_committed_past(monkeypatch):
    handled = []

    consumer = _accepting_consumer()

    def explode_on_first(raw, **_):
        handled.append(raw)
        if len(handled) == 1:
            raise RuntimeError("poison message")
        return True

    consumer._handle = explode_on_first
    errors_before = _counter("keep_automation_consumer_handle_errors_total")

    fake = _run_loop(
        consumer,
        [_FakeMessage(b"first", offset=0), _FakeMessage(b"second", offset=1)],
        monkeypatch,
    )

    assert handled == [b"first", b"second"]  # the loop survived and moved on
    assert fake.closed is True
    assert fake.committed_position(0) == 2  # poison does not wedge the partition
    assert _counter("keep_automation_consumer_handle_errors_total") == errors_before + 1


def test_undeserializable_message_is_committed_past(monkeypatch):
    fake = _run_loop(
        _accepting_consumer(), [_FakeMessage(b"not json", offset=3)], monkeypatch
    )

    assert fake.committed_position(0) == 4


def test_message_abandoned_on_shutdown_is_not_committed(monkeypatch):
    holder = {}

    client = ScriptedSubmitClient(
        failure(SubmitOutcome.API_ERROR, 503),
        on_submit=lambda _request: holder["consumer"].stop(),
    )
    consumer = build_test_consumer(
        pipeline=build_test_pipeline(redis=InMemoryRedis(), submit_client=client)
    )
    holder["consumer"] = consumer

    fake = _run_loop(consumer, [_message(0)], monkeypatch)

    assert len(client.requests) == 1
    assert fake.committed_position(0) in (None, 0)


def test_pool_saturation_pauses_but_keeps_polling_until_capacity_returns(monkeypatch):
    class SaturatedOnceWorkerPool:
        def __init__(self):
            self.capacity_checks = 0
            self.submissions = []

        def has_capacity(self):
            self.capacity_checks += 1
            return self.capacity_checks > 1

        def try_submit(self, function, *args):
            self.submissions.append((function, args))
            return True

        def shutdown(self, *, wait):
            pass

    pool = SaturatedOnceWorkerPool()
    consumer = build_test_consumer(worker_pool=pool)

    fake = _run_loop(consumer, [_FakeMessage(b"first")], monkeypatch)

    assert pool.capacity_checks >= 2
    assert len(pool.submissions) == 1
    assert fake.pause_calls == [{0}]
    assert fake.paused == set()


def test_saturation_pauses_once_and_logs_kafka_errors(monkeypatch, caplog):
    class SaturatedTwiceWorkerPool:
        def __init__(self):
            self.capacity_checks = 0

        def has_capacity(self):
            self.capacity_checks += 1
            return self.capacity_checks > 2

        def try_submit(self, function, *args):
            return True

        def shutdown(self, *, wait):
            pass

    consumer = build_test_consumer(worker_pool=SaturatedTwiceWorkerPool())

    with caplog.at_level(logging.ERROR, logger="src.core.kafka_consumer"):
        fake = _run_loop(
            consumer,
            [_FakeMessage(error="broker went away")],
            monkeypatch,
        )

    assert fake.pause_calls == [{0}]
    assert any("broker went away" in record.getMessage() for record in caplog.records)


def test_message_polled_while_paused_is_rewound_and_never_committed_past(monkeypatch):
    class NeverFreePool:
        def has_capacity(self):
            return False

        def try_submit(self, function, *args):
            raise AssertionError("nothing may be dispatched without capacity")

        def shutdown(self, *, wait):
            pass

    before = _counter("keep_automation_consumer_messages_rewound_total")
    consumer = build_test_consumer(worker_pool=NeverFreePool())

    fake = _run_loop(consumer, [_message(9)], monkeypatch, ignore_pause=True)

    assert fake.seeks == [(0, 9)]
    # Tracked as outstanding: the final commit is exactly at it, never past.
    assert fake.committed_position(0) == 9
    assert _counter("keep_automation_consumer_messages_rewound_total") == before + 1


def test_full_uncommitted_window_pauses_only_that_partition(monkeypatch):
    monkeypatch.setattr(
        kafka_consumer_module, "MAX_UNCOMMITTED_OFFSETS_PER_PARTITION", 3
    )
    pool = _DeferredWorkerPool()
    consumer = _accepting_consumer(worker_pool=pool)
    observed = {}

    def snapshot(name):
        def step(fake):
            observed[name] = set(fake.paused)

        return step

    script = [
        _message(0, partition=0),  # task 0: stuck
        _message(1, partition=0),
        _message(2, partition=0),
        _message(0, partition=1),
        lambda fake: [pool.run(i) for i in (1, 2, 3)],
        None,  # a loop iteration applies backpressure
        snapshot("stuck"),
        lambda fake: pool.run(0),
        None,
        snapshot("drained"),
    ]
    _run_loop(consumer, script, monkeypatch, partitions=(0, 1))

    assert observed["stuck"] == {0}
    assert observed["drained"] == set()


def test_revoke_commits_settled_offsets_and_stops_stale_work(monkeypatch):
    # Task 1 never runs during the revoke, so the drain gives up at its bound.
    monkeypatch.setattr(kafka_consumer_module, "IN_FLIGHT_DRAIN_TIMEOUT_SECONDS", 0.05)
    timeouts_before = _counter("keep_automation_consumer_revoke_drain_timeouts_total")
    pool = _DeferredWorkerPool()
    client = ScriptedSubmitClient()
    consumer = build_test_consumer(
        pipeline=build_test_pipeline(redis=InMemoryRedis(), submit_client=client),
        worker_pool=pool,
    )
    before = _counter(
        "keep_automation_consumer_messages_abandoned_total", reason="revoked"
    )

    script = [
        _message(0),
        _message(1),
        lambda fake: pool.run(0),
        lambda fake: fake.revoke(0),
        lambda fake: pool.run(1),  # its partition is gone: abandons, no submit
    ]
    fake = _run_loop(consumer, script, monkeypatch)

    sync_commits = [c for c, asynchronous in fake.commits if not asynchronous]
    assert {0: 1} in sync_commits
    assert len(client.requests) == 1  # only the message settled before revoke
    assert (
        _counter("keep_automation_consumer_revoke_drain_timeouts_total")
        == timeouts_before + 1
    )
    assert (
        _counter("keep_automation_consumer_messages_abandoned_total", reason="revoked")
        == before + 1
    )


def test_stale_settle_after_reassignment_is_ignored():
    tracker = OffsetTracker()
    key = (TOPIC, 0)
    stale_epoch = tracker.track(key, 5)
    tracker.revoke([key])
    tracker.assign([key])
    tracker.track(key, 5)

    assert tracker.settle(key, 5, stale_epoch) is False
    assert tracker.commits_due() == [(key, 5)]


def test_failed_async_commit_is_offered_again():
    tracker = OffsetTracker()
    consumer = build_test_consumer(offset_tracker=tracker)
    key = (TOPIC, 0)
    tracker.settle(key, 0, tracker.track(key, 0))
    tracker.mark_committed(tracker.commits_due())
    assert tracker.commits_due() == []

    class FailedPartition:
        topic, partition, error = TOPIC, 0, "REBALANCE_IN_PROGRESS"

    before = _counter("keep_automation_consumer_offset_commit_errors_total")
    consumer._on_commit(None, [FailedPartition()])

    assert tracker.commits_due() == [(key, 1)]
    assert (
        _counter("keep_automation_consumer_offset_commit_errors_total") == before + 1
    )


def test_commit_exception_does_not_mark_positions_committed():
    tracker = OffsetTracker()
    consumer = build_test_consumer(offset_tracker=tracker)
    key = (TOPIC, 0)
    tracker.settle(key, 0, tracker.track(key, 0))

    class BrokenCommit:
        def commit(self, offsets, asynchronous):
            raise KafkaException("coordinator not available")

    consumer._commit_settled(BrokenCommit(), synchronous=True)

    assert tracker.commits_due() == [(key, 1)]


def test_stop_cancels_in_flight_retries():
    consumer = build_test_consumer()
    in_flight = consumer._cancel_event((TOPIC, 0), epoch=1)

    consumer.stop()

    assert consumer._stopping.is_set()
    assert in_flight.is_set()  # a worker sleeping in backoff wakes now


def test_kafka_errors_are_logged_without_reaching_the_pipeline(monkeypatch):
    consumer = build_test_consumer()
    handled = []
    consumer._handle = lambda raw, **_: handled.append(raw)

    _run_loop(consumer, [_FakeMessage(error="broker went away")], monkeypatch)

    assert handled == []


def test_message_of_a_window_paused_partition_is_rewound_not_dispatched(monkeypatch):
    """librdkafka can hand back a prefetched message of a partition we just
    paused; dispatching it would let the window cap bound nothing."""
    monkeypatch.setattr(
        kafka_consumer_module, "MAX_UNCOMMITTED_OFFSETS_PER_PARTITION", 2
    )
    pool = _DeferredWorkerPool()
    consumer = _accepting_consumer(worker_pool=pool)

    script = [
        _message(0),  # task 0: stuck
        _message(1),
        lambda fake: pool.run(1),
        None,  # a loop iteration pauses p0 (window 2 >= 2)
        _message(2),  # delivered anyway (prefetched)
    ]
    fake = _run_loop(consumer, script, monkeypatch, ignore_pause=True)

    assert fake.seeks == [(0, 2)]
    assert len(pool.tasks) == 2  # offset 2 was never dispatched
    assert fake.committed_position(0) in (None, 0)


def test_revoke_waits_for_an_in_flight_submit_and_commits_it(monkeypatch):
    """B1: the next owner must not re-read a message whose submit is still
    running here, or the redelivery loses the cooldown claim to its sibling."""
    release = threading.Event()
    started = threading.Event()

    def block(_request):
        started.set()
        release.wait(5)

    client = ScriptedSubmitClient(ACCEPTED, on_submit=block)
    consumer = build_test_consumer(
        pipeline=build_test_pipeline(redis=InMemoryRedis(), submit_client=client),
        worker_pool=BoundedWorkerPool(4),
    )
    order = []

    def revoke_mid_submit(fake):
        assert started.wait(5)
        threading.Timer(0.2, lambda: (order.append("submit_released"), release.set())).start()
        fake.revoke(0)
        order.append("revoke_returned")

    fake = _run_loop(consumer, [_message(0), revoke_mid_submit], monkeypatch)

    assert order == ["submit_released", "revoke_returned"]
    assert {0: 1} in [c for c, asynchronous in fake.commits if not asynchronous]
    assert len(client.requests) == 1


def test_revoke_wakes_a_worker_sleeping_in_backoff(monkeypatch):
    attempted = threading.Event()

    client = ScriptedSubmitClient(
        failure(SubmitOutcome.API_ERROR, 503), on_submit=lambda _r: attempted.set()
    )
    pipeline = build_test_pipeline(
        redis=InMemoryRedis(),
        submit_client=client,
        backoff_initial_seconds=60.0,
        backoff_max_seconds=60.0,
    )
    consumer = build_test_consumer(pipeline=pipeline, worker_pool=BoundedWorkerPool(4))
    before = _counter(
        "keep_automation_consumer_messages_abandoned_total", reason="revoked"
    )
    elapsed = {}

    def revoke_during_backoff(fake):
        assert attempted.wait(5)
        time.sleep(0.1)  # the worker is now in its 30-60s backoff
        started = time.monotonic()
        fake.revoke(0)
        elapsed["revoke"] = time.monotonic() - started
        # The revoke returned only after the woken worker left the pool.
        elapsed["in_flight_after"] = dict(consumer._inflight)

    fake = _run_loop(consumer, [_message(0), revoke_during_backoff], monkeypatch)

    assert elapsed["revoke"] < 2.0
    assert elapsed["in_flight_after"] == {}
    assert len(client.requests) == 1
    assert fake.committed_position(0) in (None, 0)  # unsettled: redelivered
    assert (
        _counter("keep_automation_consumer_messages_abandoned_total", reason="revoked")
        == before + 1
    )


def test_late_settle_from_a_revoked_epoch_cannot_move_the_new_assignment():
    """A worker whose drain timed out finishes after the partition came back:
    its settle must not commit past the redelivered copy."""
    tracker = OffsetTracker()
    fake = _FakeKafkaConsumer([], on_exhausted=lambda: None)
    key = (TOPIC, 0)
    holder = {}

    def revoke_and_reassign_mid_submit(_request):
        kafka_consumer_module.IN_FLIGHT_DRAIN_TIMEOUT_SECONDS = 0.05
        consumer._on_revoke(fake, [TopicPartition(TOPIC, 0)])
        consumer._on_assign(fake, [TopicPartition(TOPIC, 0)])
        holder["new_epoch"] = tracker.track(key, 0)  # redelivered copy

    client = ScriptedSubmitClient(ACCEPTED, on_submit=revoke_and_reassign_mid_submit)
    consumer = build_test_consumer(
        pipeline=build_test_pipeline(redis=InMemoryRedis(), submit_client=client),
        offset_tracker=tracker,
    )
    original = kafka_consumer_module.IN_FLIGHT_DRAIN_TIMEOUT_SECONDS
    try:
        old_epoch = tracker.track(key, 0)
        consumer._inflight[key] = 1
        consumer._handle_safely(_message(0).value(), key, 0, old_epoch, threading.Event())
    finally:
        kafka_consumer_module.IN_FLIGHT_DRAIN_TIMEOUT_SECONDS = original

    assert holder["new_epoch"] != old_epoch
    assert tracker.commits_due() == [(key, 0)]  # not 1


def test_lost_partitions_are_forgotten_without_a_commit(monkeypatch):
    pool = _DeferredWorkerPool()
    consumer = _accepting_consumer(worker_pool=pool)
    observed = {}

    def lose(fake):
        commits_before = len(fake.commits)
        fake.lose(0)
        observed["commits_during_lost"] = len(fake.commits) - commits_before
        observed["due_after"] = consumer._tracker.commits_due()

    script = [_message(0), lambda fake: pool.run(0), lose]
    _run_loop(consumer, script, monkeypatch)

    assert observed["commits_during_lost"] == 0
    assert observed["due_after"] == []


def test_shutdown_waits_for_workers():
    calls = []

    class RecordingPool(_DeferredWorkerPool):
        def shutdown(self, *, wait):
            calls.append(wait)

    consumer = build_test_consumer(worker_pool=RecordingPool())
    fake = _FakeKafkaConsumer([], on_exhausted=consumer.stop)
    consumer._create_consumer = lambda: fake
    original = signal.signal
    signal.signal = lambda *_: None
    try:
        consumer.start()
    finally:
        signal.signal = original

    assert calls == [True]


def test_commit_callback_is_wired_into_the_kafka_client(monkeypatch):
    import confluent_kafka

    captured = {}

    class CapturingConsumer:
        def __init__(self, config):
            captured.update(config)

    monkeypatch.setattr(confluent_kafka, "Consumer", CapturingConsumer)
    consumer = build_test_consumer()

    consumer._create_consumer()

    assert captured["on_commit"] == consumer._on_commit
    assert captured["enable.auto.commit"] is False


def test_polling_continues_while_every_worker_is_blocked(monkeypatch):
    """AC7: a blocked submit never stalls the poll loop (heartbeats, rebalances)."""
    release = threading.Event()
    client = ScriptedSubmitClient(ACCEPTED, on_submit=lambda _r: release.wait(5))
    consumer = build_test_consumer(
        pipeline=build_test_pipeline(redis=InMemoryRedis(), submit_client=client),
        worker_pool=BoundedWorkerPool(1),
    )
    observed = {}

    def record(fake):
        observed["polls_while_blocked"] = fake.polls
        release.set()

    def await_second_submit(_fake):
        # Stopping cancels work not yet attempted, so let it reach the API.
        deadline = time.monotonic() + 5
        while len(client.requests) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)

    script = [_message(0), *([None] * 25), record, _message(1), await_second_submit]
    fake = _run_loop(consumer, script, monkeypatch)

    assert observed["polls_while_blocked"] >= 25
    assert len(client.requests) == 2
    assert fake.pause_calls  # saturation paused partitions meanwhile



def test_malformed_message_is_skipped_and_never_submitted(monkeypatch, caplog):
    client = ScriptedSubmitClient()
    consumer = build_test_consumer(
        pipeline=build_test_pipeline(redis=InMemoryRedis(), submit_client=client)
    )
    payload = json.loads(json.dumps(CANONICAL_MESSAGE))
    payload["tenant_id"] = None
    before = _counter("keep_automation_consumer_deserialize_errors_total")

    with caplog.at_level(logging.ERROR, logger="src.core.kafka_consumer"):
        fake = _run_loop(
            consumer, [_FakeMessage(_encode(payload), offset=4)], monkeypatch
        )

    assert client.requests == []
    assert fake.committed_position(0) == 5
    assert _counter("keep_automation_consumer_deserialize_errors_total") == before + 1
    assert "tenant_id" in caplog.text


def test_revoke_recommits_a_position_an_async_commit_may_still_lose():
    """N1: the async commit was only issued, not confirmed; the revoke must
    commit synchronously anyway rather than trust it."""
    tracker = OffsetTracker()
    consumer = build_test_consumer(offset_tracker=tracker)
    fake = _FakeKafkaConsumer([], on_exhausted=lambda: None)
    key = (TOPIC, 0)
    tracker.settle(key, 0, tracker.track(key, 0))
    consumer._commit_settled(fake, synchronous=False)
    assert fake.commits == [({0: 1}, True)]

    consumer._on_revoke(fake, [TopicPartition(TOPIC, 0)])

    assert fake.commits[-1] == ({0: 1}, False)


def test_request_level_commit_error_re_offers_every_partition():
    tracker = OffsetTracker()
    consumer = build_test_consumer(offset_tracker=tracker)
    for partition in (0, 1):
        key = (TOPIC, partition)
        tracker.settle(key, 0, tracker.track(key, 0))
    tracker.mark_committed(tracker.commits_due())

    consumer._on_commit("COORDINATOR_NOT_AVAILABLE", [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)])

    assert sorted(tracker.commits_due()) == [((TOPIC, 0), 1), ((TOPIC, 1), 1)]
