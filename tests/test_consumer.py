"""C8 skeleton tests: consumer config + matched-message parsing."""
import json

import pytest
from prometheus_client import REGISTRY

from src.core.kafka_consumer import MatchedAlertConsumer, build_consumer_config
from src.models.matched_message import MatchedAlertMessage

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

    MatchedAlertConsumer()._handle(_encode(_without("tenant_id")))

    assert (
        _counter("keep_automation_consumer_deserialize_errors_total")
        == errors_before + 1
    )
    assert (
        _counter("keep_automation_consumer_messages_consumed_total") == consumed_before
    )


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

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, _timeout):
        if self._messages:
            return self._messages.pop(0)
        self._on_exhausted()
        return None

    def close(self):
        self.closed = True


def _run_loop(consumer, messages):
    fake = _FakeKafkaConsumer(messages, on_exhausted=consumer.stop)
    consumer._create_consumer = lambda: fake
    consumer.start()
    return fake


def test_a_raising_handle_does_not_kill_the_poll_loop(monkeypatch):
    monkeypatch.setattr(MatchedAlertConsumer, "_report_gate_configuration", lambda _: None)
    handled = []

    consumer = MatchedAlertConsumer()
    def explode_on_first(raw):
        handled.append(raw)
        if len(handled) == 1:
            raise RuntimeError("poison message")

    consumer._handle = explode_on_first
    errors_before = _counter("keep_automation_consumer_handle_errors_total")

    fake = _run_loop(consumer, [_FakeMessage(b"first"), _FakeMessage(b"second")])

    assert handled == [b"first", b"second"]  # the loop survived and moved on
    assert fake.closed is True
    assert (
        _counter("keep_automation_consumer_handle_errors_total") == errors_before + 1
    )


def test_start_reports_the_gate_configuration_before_consuming():
    """The unconfigured-Redis warning must land at startup, not on message 1."""
    consumer = MatchedAlertConsumer()
    order = []

    consumer._report_gate_configuration = lambda: order.append("reported")
    consumer._handle = lambda raw: order.append("handled")

    _run_loop(consumer, [_FakeMessage(b"first")])

    assert order == ["reported", "handled"]


def test_kafka_errors_are_logged_without_reaching_the_gate(monkeypatch):
    monkeypatch.setattr(MatchedAlertConsumer, "_report_gate_configuration", lambda _: None)
    consumer = MatchedAlertConsumer()
    handled = []
    consumer._handle = handled.append

    _run_loop(consumer, [_FakeMessage(error="broker went away")])

    assert handled == []
