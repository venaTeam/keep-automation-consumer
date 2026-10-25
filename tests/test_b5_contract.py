"""Consumer regressions for the actual B5 build_messages wire shape.

The fixture's producer provenance and regeneration instructions live beside it.
The consumer assumes B5 has assigned a replay-stable per-occurrence alert.id;
it neither generates IDs nor interprets provider incident IDs.
"""
import json
from pathlib import Path

import pytest

from src.bl.gates.idempotency import IdempotencyOutcome
from src.models.matched_message import MatchedAlertMessage
from tests.gates.test_idempotency import FakeRedis, gate_with


@pytest.fixture
def payload():
    return json.loads((Path(__file__).parent / "fixtures/b5_matched_message.json").read_text())


def parse(payload):
    return MatchedAlertMessage.from_bytes(json.dumps(payload).encode())


def test_producer_payload_preserves_snapshot_and_maps_identity(payload):
    msg = parse(payload)
    assert msg.history_id == payload["alert"]["id"]
    assert "history_id" not in msg.alert
    assert msg.alert == payload["alert"]
    assert msg.cooldown == {"fields": ["site"], "seconds": 300, "scheme_ver": 1}
    assert msg.matched_m == 1


def test_confirmed_b5_message_redelivery_is_suppressed(payload):
    gate = gate_with(FakeRedis())
    assert gate.claim(parse(payload)).outcome is IdempotencyOutcome.CLAIMED
    assert gate.mark_done(parse(payload))
    assert gate.claim(parse(payload)).outcome is IdempotencyOutcome.DUPLICATE


def test_new_occurrence_same_logical_alert_is_not_suppressed(payload):
    gate = gate_with(FakeRedis())
    original = parse(payload)
    gate.claim(original)
    gate.mark_done(original)
    payload["alert"]["id"] = "evt-next-occurrence"
    new_occurrence = parse(payload)
    assert new_occurrence.fingerprint == original.fingerprint
    assert gate.claim(new_occurrence).outcome is IdempotencyOutcome.CLAIMED


def test_reusing_id_is_same_event_even_when_other_fields_change(payload):
    # Different timestamps do not override the agreed canonical event ID.
    gate = gate_with(FakeRedis())
    gate.mark_done(parse(payload))
    payload["alert"]["time_created"] = "2026-10-25T12:00:00Z"
    assert gate.claim(parse(payload)).outcome is IdempotencyOutcome.DUPLICATE


def test_idless_malformed_message_replay_never_creates_shared_key(payload):
    del payload["alert"]["id"]
    redis = FakeRedis()
    gate = gate_with(redis)
    for _ in range(2):
        decision = gate.claim(parse(payload))
        assert decision.outcome is IdempotencyOutcome.FAIL_OPEN
        assert decision.gate_flags["reason"] == "missing_history_id"
    assert redis.calls == []
