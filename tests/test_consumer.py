"""C8 skeleton tests: consumer config + matched-message parsing."""
import json
import logging
import signal

import pytest
from prometheus_client import REGISTRY

from src.bl.gates.cooldown import CooldownGate
from src.core.kafka_consumer import build_consumer_config
from src.models.matched_message import MatchedAlertMessage
from tests.helpers import build_test_consumer

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


def _counter(name: str) -> float:
    return REGISTRY.get_sample_value(name) or 0.0


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

    build_test_consumer()._handle(_encode(_without("tenant_id")))

    assert (
        _counter("keep_automation_consumer_deserialize_errors_total")
        == errors_before + 1
    )
    assert (
        _counter("keep_automation_consumer_messages_consumed_total") == consumed_before
    )


def test_c10_cooldown_gate_remains_dark(monkeypatch):
    """C11, not C10, owns runtime gate composition and offset semantics."""

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("C10 must remain unwired until C11")

    monkeypatch.setattr(CooldownGate, "claim", fail_if_called)

    # The canonical message carries cooldown config. Processing it today must
    # still stop after the already-wired C9 decision without touching C10.
    build_test_consumer()._handle(_encode(CANONICAL_MESSAGE))


# -- poll loop resilience -----------------------------------------------------
#
# `start()` is the outermost guard: whatever escapes `_handle` decides between
# "skip one message" and "exit the process, get redelivered the same poison
# message on restart, and stall the partition". C11 adds submit + offset commit
# inside that same path, so the guard has to be pinned before it does.


class _FakeMessage:
    def __init__(self, value=b"{}", error=None):
        self._value = value
        self._error = error

    def error(self):
        return self._error

    def value(self):
        return self._value


class _FakeKafkaConsumer:
    """Yields a scripted list of poll() results, then stops the loop."""

    def __init__(self, messages, on_exhausted):
        self._messages = list(messages)
        self._on_exhausted = on_exhausted
        self.subscribed = None
        self.closed = False
        self.paused = False
        self.pause_count = 0

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, _timeout):
        if self.paused:
            if self._messages and self._messages[0].error():
                return self._messages.pop(0)
            return None
        if self._messages:
            return self._messages.pop(0)
        self._on_exhausted()
        return None

    def close(self):
        self.closed = True

    def assignment(self):
        return ["partition-0"]

    def pause(self, _partitions):
        self.paused = True
        self.pause_count += 1

    def resume(self, _partitions):
        self.paused = False


def _run_loop(consumer, messages, monkeypatch):
    # `start()` installs process-global SIGINT/SIGTERM handlers. Left in place,
    # the pytest session's Ctrl-C would end up bound to a dead consumer's
    # stop().
    monkeypatch.setattr(signal, "signal", lambda *_: None)
    fake = _FakeKafkaConsumer(messages, on_exhausted=consumer.stop)
    consumer._create_consumer = lambda: fake
    consumer.start()
    return fake


def test_a_raising_handle_does_not_kill_the_poll_loop(monkeypatch):
    handled = []

    consumer = build_test_consumer()
    def explode_on_first(raw):
        handled.append(raw)
        if len(handled) == 1:
            raise RuntimeError("poison message")

    consumer._handle = explode_on_first
    errors_before = _counter("keep_automation_consumer_handle_errors_total")

    fake = _run_loop(consumer, [_FakeMessage(b"first"), _FakeMessage(b"second")], monkeypatch)

    assert handled == [b"first", b"second"]  # the loop survived and moved on
    assert fake.closed is True
    assert (
        _counter("keep_automation_consumer_handle_errors_total") == errors_before + 1
    )


def test_poll_loop_dispatches_message_processing_to_worker_pool(monkeypatch):
    class RecordingWorkerPool:
        def __init__(self):
            self.submissions = []

        def has_capacity(self):
            return True

        def try_submit(self, function, *args):
            self.submissions.append((function, args))
            return True

        def shutdown(self, *, wait):
            pass

    pool = RecordingWorkerPool()
    consumer = build_test_consumer(worker_pool=pool)

    _run_loop(consumer, [_FakeMessage(b"first")], monkeypatch)

    assert len(pool.submissions) == 1
    function, args = pool.submissions[0]
    assert function == consumer._handle_safely
    assert args == (b"first",)


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
    assert fake.paused is False


def test_saturation_reapplies_pause_and_logs_kafka_errors(monkeypatch, caplog):
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

    assert fake.pause_count == 2
    assert any("broker went away" in record.getMessage() for record in caplog.records)


def test_workers_drain_before_kafka_consumer_closes(monkeypatch):
    events = []

    class OrderedWorkerPool:
        def has_capacity(self):
            return True

        def try_submit(self, function, *args):
            function(*args)
            return True

        def shutdown(self, *, wait):
            events.append("workers_drained")

    consumer = build_test_consumer(worker_pool=OrderedWorkerPool())
    monkeypatch.setattr(signal, "signal", lambda *_: None)
    fake = _FakeKafkaConsumer([], on_exhausted=consumer.stop)
    fake.close = lambda: events.append("kafka_closed")
    consumer._create_consumer = lambda: fake

    consumer.start()

    assert events == ["workers_drained", "kafka_closed"]


def test_kafka_errors_are_logged_without_reaching_the_gate(monkeypatch):
    consumer = build_test_consumer()
    handled = []
    consumer._handle = handled.append

    _run_loop(consumer, [_FakeMessage(error="broker went away")], monkeypatch)

    assert handled == []
