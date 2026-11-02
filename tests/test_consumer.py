"""Consumer tests: config, message parsing, and the C11 poll loop (dispatch,
backpressure, contiguous commits, rebalance, shutdown)."""
import json
import logging
import signal

import pytest
from confluent_kafka import KafkaException, TopicPartition
from prometheus_client import REGISTRY

import src.core.kafka_consumer as kafka_consumer_module
from src.core.kafka_consumer import build_consumer_config
from src.core.offset_tracker import OffsetTracker
from src.models.matched_message import MatchedAlertMessage
from src.models.submit import SubmitOutcome
from tests.helpers import (
    InMemoryRedis,
    ScriptedSubmitClient,
    build_test_consumer,
    build_test_pipeline,
    failure,
)

# The canonical matched message, copied from automation-contracts.md
# section "Matched message". That doc is authoritative — when it changes,
# this fixture changes with it, never the other way round.
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
        "history_id": "evt-789",
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

    def subscribe(self, topics, on_assign=None, on_revoke=None, on_lost=None):
        self.subscribed = topics
        self.callbacks = {"assign": on_assign, "revoke": on_revoke, "lost": on_lost}

    def poll(self, _timeout):
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
    payload["alert"]["history_id"] = f"evt-{partition}-{offset}"
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
    assert fake.committed_position(0) in (None, 9)
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

    consumer.stop()

    assert consumer._stopping.is_set()


def test_kafka_errors_are_logged_without_reaching_the_pipeline(monkeypatch):
    consumer = build_test_consumer()
    handled = []
    consumer._handle = lambda raw, **_: handled.append(raw)

    _run_loop(consumer, [_FakeMessage(error="broker went away")], monkeypatch)

    assert handled == []
