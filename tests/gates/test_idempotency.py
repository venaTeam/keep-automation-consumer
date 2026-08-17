"""C9 idempotency-gate tests.

The decision table under test is `automation-contracts.md` §Redis keys /
spec §5.2 step 1, line by line:

    NX ok -> claimed          | NX fail + `done`    -> duplicate (suppress)
    NX fail + `pending` -> ambiguous (submit)       | key gone -> ambiguous (submit)
    Redis unreachable/unset   -> fail open (submit)

Redis is faked in-process: these are contract tests, not a driver integration.
"""

import json

import pytest
from prometheus_client import REGISTRY

from src.bl.gates.idempotency import (
    IdempotencyGate,
    IdempotencyOutcome,
    idempotency_key,
)
from src.bl.suppression_audit import REASON_DUPLICATE
from src.config.consts import IDEMPOTENCY_TTL_SECONDS
from src.core.kafka_consumer import MatchedAlertConsumer
from src.models.matched_message import MatchedAlertMessage

AUTOMATION_ID = "3f2b8c1e-4d6a-4b2f-9e77-0a1b2c3d4e5f"
HISTORY_ID = "evt-789"

MESSAGE = {
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
        "history_id": HISTORY_ID,
        "time_created": "2026-07-12T14:03:00Z",
    },
    "automation_id": AUTOMATION_ID,
    "matched_m": 2,
    "cooldown": None,
}


class FakeRedis:
    """Just enough Redis: `SET [NX] [EX]` and `GET`, plus failure injection."""

    def __init__(self, fail_on=()):
        self.values = {}
        self.ttls = {}
        self.fail_on = set(fail_on)
        self.calls = []

    def set(self, key, value, nx=False, ex=None):
        self.calls.append(("set", key, value, nx, ex))
        if "set" in self.fail_on:
            raise ConnectionError("redis down")
        if nx and key in self.values:
            return None  # redis-py returns None when NX does not apply
        self.values[key] = value
        self.ttls[key] = ex
        return True

    def get(self, key):
        self.calls.append(("get", key))
        if "get" in self.fail_on:
            raise ConnectionError("redis down")
        return self.values.get(key)


class RecordingAuditor:
    def __init__(self):
        self.rows = []

    def record_suppression(self, **row):
        self.rows.append(row)


def message(**overrides) -> MatchedAlertMessage:
    payload = json.loads(json.dumps(MESSAGE))
    payload.update(overrides)
    return MatchedAlertMessage.from_bytes(json.dumps(payload).encode("utf-8"))


def gate_with(client) -> IdempotencyGate:
    return IdempotencyGate(client_factory=lambda: client)


def counter(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


# -- key format ---------------------------------------------------------------


def test_key_matches_the_contract_verbatim():
    assert idempotency_key(HISTORY_ID, AUTOMATION_ID) == f"idem:{HISTORY_ID}:{AUTOMATION_ID}"


def test_key_is_event_scoped_not_fingerprint_scoped():
    """Two events of the same logical alert must not share a gate."""
    fingerprint = MESSAGE["alert"]["fingerprint"]
    key = idempotency_key(HISTORY_ID, AUTOMATION_ID)

    assert fingerprint not in key
    assert idempotency_key("evt-790", AUTOMATION_ID) != key


# -- the decision table -------------------------------------------------------


def test_first_claim_is_claimed_and_stores_pending():
    redis = FakeRedis()

    decision = gate_with(redis).claim(message())

    assert decision.outcome is IdempotencyOutcome.CLAIMED
    assert decision.should_submit is True
    assert redis.values[decision.key] == "pending"


def test_claim_ttl_defaults_to_the_contract_24h():
    redis = FakeRedis()

    decision = gate_with(redis).claim(message())

    assert redis.ttls[decision.key] == IDEMPOTENCY_TTL_SECONDS
    # The env default is the contract value (spec §7.6), so an unset
    # IDEMPOTENCY_TTL_SECONDS is always the contract-correct deployment.
    assert IDEMPOTENCY_TTL_SECONDS == 86400


@pytest.mark.parametrize(
    "env_value, expected",
    [
        ("3600", 3600),  # overridable per environment
        ("0", 1),        # floored: `EX 0` is a Redis error, not a short TTL
        ("-5", 1),
    ],
)
def test_ttl_env_override_is_read_and_floored(monkeypatch, env_value, expected):
    import importlib

    from src.config import consts

    monkeypatch.setenv("IDEMPOTENCY_TTL_SECONDS", env_value)
    try:
        assert importlib.reload(consts).IDEMPOTENCY_TTL_SECONDS == expected
    finally:
        monkeypatch.delenv("IDEMPOTENCY_TTL_SECONDS")
        importlib.reload(consts)


def test_claim_uses_nx_so_it_never_overwrites_a_sibling_claim():
    redis = FakeRedis()
    gate = gate_with(redis)
    gate.claim(message())
    gate.mark_done(message())

    gate.claim(message())  # a redelivery must not reset `done` back to `pending`

    assert redis.values[idempotency_key(HISTORY_ID, AUTOMATION_ID)] == "done"


def test_done_suppresses_as_duplicate():
    redis = FakeRedis()
    key = idempotency_key(HISTORY_ID, AUTOMATION_ID)
    redis.values[key] = "done"

    decision = gate_with(redis).claim(message())

    assert decision.outcome is IdempotencyOutcome.DUPLICATE
    assert decision.should_submit is False


def test_pending_proceeds_rather_than_dropping():
    redis = FakeRedis()
    redis.values[idempotency_key(HISTORY_ID, AUTOMATION_ID)] = "pending"

    decision = gate_with(redis).claim(message())

    assert decision.outcome is IdempotencyOutcome.AMBIGUOUS
    assert decision.should_submit is True


def test_key_vanishing_between_set_and_get_proceeds():
    """Expiry can land between the NX and the GET — submit, never drop."""

    class VanishingRedis(FakeRedis):
        def set(self, key, value, nx=False, ex=None):
            self.values[key] = value  # pretend a sibling holds the key
            return None if nx else True

        def get(self, key):
            self.values.pop(key, None)  # ...and it expires right now
            return None

    decision = gate_with(VanishingRedis()).claim(message())

    assert decision.outcome is IdempotencyOutcome.AMBIGUOUS
    assert decision.should_submit is True


def test_bytes_value_is_still_read_as_done():
    """A client built without `decode_responses` must not read `done` as new."""
    redis = FakeRedis()
    redis.values[idempotency_key(HISTORY_ID, AUTOMATION_ID)] = b"done"

    assert gate_with(redis).claim(message()).outcome is IdempotencyOutcome.DUPLICATE


@pytest.mark.parametrize(
    "outcome",
    [
        IdempotencyOutcome.CLAIMED,
        IdempotencyOutcome.AMBIGUOUS,
        IdempotencyOutcome.FAIL_OPEN,
    ],
)
def test_only_a_confirmed_duplicate_stops_a_submit(outcome):
    from src.bl.gates.idempotency import IdempotencyDecision

    assert IdempotencyDecision(outcome=outcome, key="k").should_submit is True


# -- fail-open ----------------------------------------------------------------


def test_redis_unconfigured_fails_open():
    decision = gate_with(None).claim(message())

    assert decision.outcome is IdempotencyOutcome.FAIL_OPEN
    assert decision.gate_flags == {
        "idempotency": "skipped",
        "reason": "redis_unavailable",
    }


def test_claim_error_fails_open_and_counts_it():
    before = counter(
        "keep_automation_consumer_redis_errors_total", operation="claim"
    )

    decision = gate_with(FakeRedis(fail_on=["set"])).claim(message())

    assert decision.outcome is IdempotencyOutcome.FAIL_OPEN
    assert decision.gate_flags["reason"] == "redis_unavailable"
    assert (
        counter("keep_automation_consumer_redis_errors_total", operation="claim")
        == before + 1
    )


def test_read_error_after_nx_fail_fails_open():
    redis = FakeRedis(fail_on=["get"])
    redis.values[idempotency_key(HISTORY_ID, AUTOMATION_ID)] = "done"

    decision = gate_with(redis).claim(message())

    assert decision.outcome is IdempotencyOutcome.FAIL_OPEN
    assert decision.should_submit is True


def test_message_without_history_id_fails_open_and_arms_nothing():
    redis = FakeRedis()
    alert = dict(MESSAGE["alert"])
    alert.pop("history_id")

    decision = gate_with(redis).claim(message(alert=alert))

    assert decision.outcome is IdempotencyOutcome.FAIL_OPEN
    assert decision.gate_flags["reason"] == "missing_history_id"
    assert redis.calls == []  # no partial key ever written


# -- mark_done ----------------------------------------------------------------


def test_mark_done_writes_done_with_a_fresh_ttl():
    redis = FakeRedis()
    gate = gate_with(redis)
    decision = gate.claim(message())

    assert gate.mark_done(message()) is True
    assert redis.values[decision.key] == "done"
    assert redis.ttls[decision.key] == IDEMPOTENCY_TTL_SECONDS


def test_mark_done_survives_a_dead_redis():
    assert gate_with(FakeRedis(fail_on=["set"])).mark_done(message()) is False
    assert gate_with(None).mark_done(message()) is False


# -- decision metric ----------------------------------------------------------


def test_every_decision_is_counted_by_outcome():
    before = counter(
        "keep_automation_consumer_idempotency_decisions_total", outcome="claimed"
    )

    gate_with(FakeRedis()).claim(message())

    assert (
        counter(
            "keep_automation_consumer_idempotency_decisions_total", outcome="claimed"
        )
        == before + 1
    )


# -- redelivery through the consume loop --------------------------------------


def test_redelivery_of_the_same_event_yields_one_submit_and_one_suppression():
    """The C9 half of the story's integration check.

    Delivery 1 claims and (once C11 lands) submits; `mark_done` stands in for
    the API-confirmed submit. Delivery 2 is suppressed and audited exactly once.
    """
    redis = FakeRedis()
    gate = gate_with(redis)
    auditor = RecordingAuditor()
    consumer = MatchedAlertConsumer(idempotency_gate=gate, suppression_auditor=auditor)
    raw = json.dumps(MESSAGE).encode("utf-8")

    consumer._handle(raw)
    assert auditor.rows == []  # first delivery proceeds

    gate.mark_done(message())  # stands in for the confirmed submit (C11)

    consumer._handle(raw)
    consumer._handle(raw)

    assert len(auditor.rows) == 2  # every redelivery is audited, none submitted
    assert {row["reason"] for row in auditor.rows} == {REASON_DUPLICATE}
    assert auditor.rows[0]["tenant_id"] == "keep"
    assert auditor.rows[0]["history_id"] == HISTORY_ID
    assert auditor.rows[0]["fingerprint"] == "abc123"


def test_a_pending_sibling_does_not_suppress_the_redelivery():
    """A crash between claim and submit must not lose the automation."""
    redis = FakeRedis()
    auditor = RecordingAuditor()
    consumer = MatchedAlertConsumer(
        idempotency_gate=gate_with(redis), suppression_auditor=auditor
    )
    raw = json.dumps(MESSAGE).encode("utf-8")

    consumer._handle(raw)
    consumer._handle(raw)

    assert auditor.rows == []
