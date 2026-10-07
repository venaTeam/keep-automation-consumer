"""C9 idempotency-gate tests.

The decision table under test is `automation-contracts.md` §Redis keys /
spec §5.2 step 1, line by line:

    NX ok -> claimed          | NX fail + `done`    -> duplicate (suppress)
    NX fail + `pending` -> ambiguous (submit)       | key gone -> ambiguous (submit)
    Redis unreachable/unset   -> fail open (submit)

Redis is faked in-process: these are contract tests, not a driver integration.
"""

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from prometheus_client import REGISTRY

from src.bl.gates.idempotency import (
    IdempotencyGate,
    IdempotencyOutcome,
    idempotency_key,
)
from src.bl.suppression_audit import REASON_DUPLICATE
from src.config.consts import (
    IDEMPOTENCY_TTL_SECONDS,
    REDIS_BREAKER_FAILURE_THRESHOLD,
    REDIS_BREAKER_OPEN_SECONDS,
    REDIS_ERROR_LOG_INTERVAL_SECONDS,
)
from src.core.metrics import GateConfigSetting, RedisOperation
from src.models.matched_message import MatchedAlertMessage
from tests.helpers import build_test_consumer

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
        "id": HISTORY_ID,
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
    """Explicit signature on purpose: a `**row` fake would accept a renamed
    parameter that the real `LoggingSuppressionAuditor` rejects, hiding a
    TypeError that only fires on the first production duplicate."""

    def __init__(self):
        self.rows = []

    def record_suppression(
        self,
        *,
        tenant_id,
        automation_id,
        history_id,
        fingerprint,
        reason,
        gate_flags=None,
    ):
        self.rows.append(
            {
                "tenant_id": tenant_id,
                "automation_id": automation_id,
                "history_id": history_id,
                "fingerprint": fingerprint,
                "reason": reason,
                "gate_flags": gate_flags,
            }
        )


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


def test_legacy_confirmation_suppresses_b5_redelivery_with_same_id():
    redis = FakeRedis()
    gate = gate_with(redis)
    legacy_alert = dict(MESSAGE["alert"])
    legacy_alert["history_id"] = legacy_alert.pop("id")
    legacy = message(alert=legacy_alert)

    assert gate.claim(legacy).outcome is IdempotencyOutcome.CLAIMED
    assert gate.mark_done(legacy) is True
    assert gate.claim(message()).outcome is IdempotencyOutcome.DUPLICATE


def test_b5_id_controls_claim_and_confirmation_when_legacy_id_differs():
    redis = FakeRedis()
    gate = gate_with(redis)
    mixed = message(alert={**MESSAGE["alert"], "history_id": "legacy-event"})

    decision = gate.claim(mixed)
    assert decision.key == idempotency_key(HISTORY_ID, AUTOMATION_ID)
    assert gate.mark_done(mixed) is True
    assert gate.claim(message()).outcome is IdempotencyOutcome.DUPLICATE
    assert idempotency_key("legacy-event", AUTOMATION_ID) not in redis.values

    # A new event with the same fingerprint must still proceed.
    different_event = message(alert={**MESSAGE["alert"], "id": "evt-790"})
    assert gate.claim(different_event).outcome is IdempotencyOutcome.CLAIMED


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


def test_message_without_event_identity_fails_open_and_arms_nothing():
    redis = FakeRedis()
    alert = dict(MESSAGE["alert"])
    alert.pop("id")

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


def test_mark_done_survives_a_raising_client_factory():
    """Same guard as `claim`: the factory call belongs inside the try."""

    def angry_factory():
        raise RuntimeError("client construction blew up")

    assert IdempotencyGate(client_factory=angry_factory).mark_done(message()) is False


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


def test_each_duplicate_redelivery_is_suppressed_and_audited():
    """The C9 half of the story's integration check.

    Delivery 1 claims and (once C11 lands) submits; `mark_done` stands in for
    the API-confirmed submit. Both later redeliveries are suppressed and audited.
    """
    redis = FakeRedis()
    gate = gate_with(redis)
    auditor = RecordingAuditor()
    consumer = build_test_consumer(
        idempotency_gate=gate,
        suppression_auditor=auditor,
    )
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


def test_a_failing_client_factory_does_not_escape_handle():
    """The gate's "nothing raises" contract must not depend on another module."""

    def angry_factory():
        raise RuntimeError("client construction blew up")

    consumer = build_test_consumer(
        idempotency_gate=IdempotencyGate(client_factory=angry_factory),
        suppression_auditor=RecordingAuditor(),
    )
    before = counter(
        "keep_automation_consumer_idempotency_decisions_total",
        outcome=IdempotencyOutcome.FAIL_OPEN.value,
    )

    consumer._handle(json.dumps(MESSAGE).encode("utf-8"))  # must not raise

    assert (
        counter(
            "keep_automation_consumer_idempotency_decisions_total",
            outcome=IdempotencyOutcome.FAIL_OPEN.value,
        )
        == before + 1
    )


def test_a_failing_auditor_does_not_escape_handle(caplog):
    """D17 swaps the stub for an HTTP client that will raise on a 503.

    An exception here would exit the process, the uncommitted offset would be
    redelivered to the restarted pod, and the same message would poison it
    again — a crashloop that stalls the partition.
    """

    class AngryAuditor:
        def record_suppression(self, **_):
            raise RuntimeError("audit API 503")

    redis = FakeRedis()
    redis.values[idempotency_key(HISTORY_ID, AUTOMATION_ID)] = "done"
    consumer = build_test_consumer(
        idempotency_gate=gate_with(redis), suppression_auditor=AngryAuditor()
    )

    with caplog.at_level(logging.ERROR, logger="src.core.kafka_consumer"):
        consumer._handle(json.dumps(MESSAGE).encode("utf-8"))  # must not raise

    assert "could not audit a suppressed duplicate" in caplog.records[-1].getMessage()


# -- circuit breaker ----------------------------------------------------------


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def failing_gate(clock):
    redis = FakeRedis(fail_on=["set"])
    return IdempotencyGate(client_factory=lambda: redis, clock=clock), redis


def test_breaker_opens_after_repeated_failures_and_stops_touching_redis():
    """Socket timeouts bound one message; only the breaker bounds throughput."""
    clock = Clock()
    gate, redis = failing_gate(clock)

    for _ in range(REDIS_BREAKER_FAILURE_THRESHOLD):
        assert gate.claim(message()).outcome is IdempotencyOutcome.FAIL_OPEN

    calls_before = len(redis.calls)
    for _ in range(20):
        assert gate.claim(message()).outcome is IdempotencyOutcome.FAIL_OPEN

    assert redis.calls[calls_before:] == []  # not one socket touched


def test_repeated_get_failures_open_the_breaker():
    """A successful NX miss must not erase a failure from the following GET."""
    clock = Clock()
    redis = FakeRedis(fail_on=["get"])
    redis.values[idempotency_key(HISTORY_ID, AUTOMATION_ID)] = "pending"
    gate = IdempotencyGate(client_factory=lambda: redis, clock=clock)

    for _ in range(REDIS_BREAKER_FAILURE_THRESHOLD):
        assert gate.claim(message()).outcome is IdempotencyOutcome.FAIL_OPEN

    calls_before = len(redis.calls)
    assert gate.claim(message()).outcome is IdempotencyOutcome.FAIL_OPEN

    assert redis.calls[calls_before:] == []


def test_breaker_stops_calling_a_raising_client_factory():
    clock = Clock()
    calls = []

    def angry_factory():
        calls.append(True)
        raise RuntimeError("client construction blew up")

    gate = IdempotencyGate(client_factory=angry_factory, clock=clock)
    for _ in range(REDIS_BREAKER_FAILURE_THRESHOLD):
        gate.claim(message())

    calls_before = len(calls)
    gate.claim(message())

    assert len(calls) == calls_before


def test_breaker_half_opens_after_the_window_and_recovers():
    clock = Clock()
    redis = FakeRedis(fail_on=["set"])
    gate = IdempotencyGate(client_factory=lambda: redis, clock=clock)

    for _ in range(REDIS_BREAKER_FAILURE_THRESHOLD):
        gate.claim(message())
    clock.advance(REDIS_BREAKER_OPEN_SECONDS + 1)

    redis.fail_on = set()  # Redis is healthy again
    decision = gate.claim(message())

    assert decision.outcome is IdempotencyOutcome.CLAIMED


def test_a_success_resets_the_failure_run():
    """Sporadic failures spread over time must never trip the breaker."""
    clock = Clock()
    redis = FakeRedis()
    gate = IdempotencyGate(client_factory=lambda: redis, clock=clock)

    for _ in range(REDIS_BREAKER_FAILURE_THRESHOLD * 3):
        redis.fail_on = {"set"}
        gate.claim(message())
        redis.fail_on = set()
        gate.claim(message())

    calls_before = len(redis.calls)
    gate.claim(message())

    assert len(redis.calls) > calls_before  # breaker never opened


def test_one_failed_probe_reopens_the_breaker():
    """The half-open probe is one message per window, not THRESHOLD of them."""
    clock = Clock()
    gate, redis = failing_gate(clock)
    for _ in range(REDIS_BREAKER_FAILURE_THRESHOLD):
        gate.claim(message())

    clock.advance(REDIS_BREAKER_OPEN_SECONDS + 1)
    calls_before = len(redis.calls)
    for _ in range(10):
        gate.claim(message())

    # Exactly one socket touch: the probe. The other nine short-circuited.
    assert len(redis.calls) == calls_before + 1


def test_redis_errors_are_logged_once_per_budget_window(caplog):
    """A Redis outage at 200 msg/s must not write 200 tracebacks/s."""
    clock = Clock()
    gate, _ = failing_gate(clock)

    with caplog.at_level(logging.ERROR, logger="src.bl.gates.idempotency"):
        gate.claim(message())
        gate.claim(message())
        gate.claim(message())
        assert len(caplog.records) == 1

        clock.advance(REDIS_ERROR_LOG_INTERVAL_SECONDS + 1)
        gate.claim(message())

    assert len(caplog.records) == 2


def test_concurrent_redis_errors_share_one_log_budget(caplog):
    worker_count = 8
    barrier = threading.Barrier(worker_count)

    class SimultaneouslyFailingRedis(FakeRedis):
        def set(self, key, value, nx=False, ex=None):
            self.calls.append(("set", key, value, nx, ex))
            barrier.wait(timeout=2)
            raise ConnectionError("redis down")

    gate = IdempotencyGate(
        client_factory=lambda: SimultaneouslyFailingRedis(),
        clock=Clock(),
    )

    with caplog.at_level(logging.ERROR, logger="src.bl.gates.idempotency"):
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            decisions = list(executor.map(lambda _: gate.claim(message()), range(worker_count)))

    assert all(
        decision.outcome is IdempotencyOutcome.FAIL_OPEN for decision in decisions
    )
    assert len(caplog.records) == 1


def test_mark_done_is_skipped_while_the_breaker_is_open():
    clock = Clock()
    gate, redis = failing_gate(clock)
    for _ in range(REDIS_BREAKER_FAILURE_THRESHOLD):
        gate.claim(message())

    calls_before = len(redis.calls)

    assert gate.mark_done(message()) is False
    assert len(redis.calls) == calls_before


# -- configuration signal -----------------------------------------------------


def test_unconfigured_redis_raises_the_config_gauge_and_warns(monkeypatch, caplog):
    import src.consumer_main as consumer_main

    monkeypatch.setattr(consumer_main, "redis_configured", lambda: False)
    with caplog.at_level(logging.WARNING, logger="src.consumer_main"):
        assert consumer_main.configure_redis() is None

    assert gauge(GateConfigSetting.REDIS_URL.value) == 1
    assert gauge(GateConfigSetting.REDIS_CLIENT.value) == 1
    assert any("REDIS_URL is not set" in r.getMessage() for r in caplog.records)


def gauge(setting: str):
    return REGISTRY.get_sample_value(
        "keep_automation_consumer_gates_config_missing", {"setting": setting}
    )


def test_an_unbuildable_client_is_reported_as_ungated(monkeypatch, caplog):
    """URL set but client unbuildable = 100% ungated, and `redis_errors` never
    increments because nothing is ever attempted. This gauge is the only signal."""
    import src.consumer_main as consumer_main

    monkeypatch.setattr(consumer_main, "redis_configured", lambda: True)
    monkeypatch.setattr(consumer_main, "get_redis_client", lambda: None)
    with caplog.at_level(logging.WARNING, logger="src.consumer_main"):
        assert consumer_main.configure_redis() is None

    assert gauge(GateConfigSetting.REDIS_CLIENT.value) == 1
    assert gauge(GateConfigSetting.REDIS_URL.value) == 0
    assert any("could not be built" in r.getMessage() for r in caplog.records)


def test_configured_redis_clears_both_config_gauges(monkeypatch):
    import src.consumer_main as consumer_main

    client = object()
    monkeypatch.setattr(consumer_main, "redis_configured", lambda: True)
    monkeypatch.setattr(consumer_main, "get_redis_client", lambda: client)

    assert consumer_main.configure_redis() is client
    assert gauge(GateConfigSetting.REDIS_URL.value) == 0
    assert gauge(GateConfigSetting.REDIS_CLIENT.value) == 0


def test_error_and_decision_counters_exist_before_the_first_event():
    """An alert on an absent series cannot distinguish "healthy" from "not deployed"."""
    for operation in RedisOperation:
        assert (
            REGISTRY.get_sample_value(
                "keep_automation_consumer_redis_errors_total",
                {"operation": operation.value},
            )
            is not None
        )
    for outcome in IdempotencyOutcome:
        assert (
            REGISTRY.get_sample_value(
                "keep_automation_consumer_idempotency_decisions_total",
                {"outcome": outcome.value},
            )
            is not None
        )


def test_a_pending_sibling_does_not_suppress_the_redelivery():
    """A crash between claim and submit must not lose the automation."""
    redis = FakeRedis()
    auditor = RecordingAuditor()
    consumer = build_test_consumer(
        idempotency_gate=gate_with(redis), suppression_auditor=auditor
    )
    raw = json.dumps(MESSAGE).encode("utf-8")

    consumer._handle(raw)
    consumer._handle(raw)

    assert auditor.rows == []
