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
