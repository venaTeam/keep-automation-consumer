"""C10 cooldown contract and gate tests."""

import json
from datetime import datetime, timezone

import pytest

from src.bl.gates.cooldown import CooldownDecision, CooldownGate
from src.contracts.cooldown import (
    COOLDOWN_PROVISIONAL_TTL_SECONDS,
    COOLDOWN_SCHEME_VER,
    canonical_cooldown_bytes,
    canonical_cooldown_hash,
    cooldown_key,
)
from src.models.cooldown import CooldownOutcome
from src.models.matched_message import MatchedAlertMessage

AUTOMATION_ID = "3f2b8c1e-4d6a-4b2f-9e77-0a1b2c3d4e5f"
RUN_ID = "9110f61d-27a2-4240-9089-4532f0b9d217"
WINNER_RUN_ID = "c486bfc1-dde9-4354-bde4-a7a3164498a0"
LOSER_RUN_ID = "d8b6193d-c830-42c3-b058-c189078e701f"
STALE_RUN_ID = "83cb0bc0-df97-427b-9933-462318199326"
REPLACEMENT_RUN_ID = "3158fa2a-0413-473a-a7ca-a7a43fabde63"

BASE_MESSAGE = {
    "tenant_id": "keep",
    "alert": {
        "application": "payments",
        "component": "api",
        "status": "firing",
        "site": "us-east-1",
        "severity": "critical",
        "environment": "prod",
        "operator": "team-payments",
        "node_name": "api-01",
        "fingerprint": "abc123",
        "history_id": "evt-789",
        "time_created": "2026-07-12T14:03:00Z",
    },
    "automation_id": AUTOMATION_ID,
    "matched_m": 2,
    "cooldown": {
        "fields": ["site", "node_name"],
        "seconds": 300,
        "scheme_ver": 1,
    },
}


class FakeRedis:
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
            return None
        self.values[key] = value
        self.ttls[key] = ex
        return True

    def ttl(self, key):
        self.calls.append(("ttl", key))
        if "ttl" in self.fail_on:
            raise ConnectionError("redis down")
        return self.ttls.get(key, -2)

    def register_script(self, script):
        if "register_script" in self.fail_on:
            raise ConnectionError("redis down")
        operation = "activate" if "EXPIRE" in script else "release"
        self.calls.append(("register_script", operation))

        def execute(*, keys, args, client=None):
            del client
            key = keys[0]
            self.calls.append((operation, key, *args))
            if operation in self.fail_on:
                raise ConnectionError("redis down")
            if self.values.get(key) != args[0]:
                return 0
            if operation == "activate":
                self.ttls[key] = int(args[1])
                return 1
            del self.values[key]
            self.ttls.pop(key, None)
            return 1

        return execute


def message(*, cooldown=None, alert=None) -> MatchedAlertMessage:
    payload = json.loads(json.dumps(BASE_MESSAGE))
    if cooldown is not None:
        payload["cooldown"] = cooldown
    if alert is not None:
        payload["alert"] = alert
    return MatchedAlertMessage.from_bytes(json.dumps(payload).encode())


def gate(redis, *, wall_clock=lambda: 1_700_000_000.0) -> CooldownGate:
    return CooldownGate(
        client_factory=lambda: redis,
        wall_clock=wall_clock,
        error_log_interval_seconds=0,
    )


# -- canonicalization --------------------------------------------------------


def test_canonicalization_sorts_fields_and_pins_literal_bytes_and_hash():
    alert = {"site": "us-east-1", "node_name": "api-01"}

    encoded = canonical_cooldown_bytes(alert, ["site", "node_name"])

    assert encoded == b'[["node_name","api-01"],["site","us-east-1"]]'
    assert (
        canonical_cooldown_hash(alert, ["site", "node_name"])
        == "5a82e86673179d7e8044580abc3ef8276ecca9a1c6d2614f1093425e46129429"
    )
    assert canonical_cooldown_hash(alert, ["node_name", "site"]) == (
        canonical_cooldown_hash(alert, ["site", "node_name"])
    )


def test_canonicalization_empty_fields_is_whole_automation_vector():
    assert canonical_cooldown_bytes({}, []) == b"[]"
    assert (
        canonical_cooldown_hash({}, [])
        == "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
    )


def test_canonicalization_is_utf8_and_delimiter_safe():
    alert = {"site": "תל-אביב", "component": "api|worker"}

    encoded = canonical_cooldown_bytes(alert, ["site", "component"])

    assert encoded.hex() == (
        "5b5b22636f6d706f6e656e74222c226170697c776f726b6572225d2c"
        "5b2273697465222c22d7aad79c2dd790d791d799d791225d5d"
    )
    assert (
        canonical_cooldown_hash(alert, ["site", "component"])
        == "73f6934017343933eb1848a2e073d69ba1b616c1c0ff827051174f9b36e17fe0"
    )


def test_key_matches_contract_and_excludes_history_id():
    entity_hash = canonical_cooldown_hash(BASE_MESSAGE["alert"], ["site"])
    key = cooldown_key(AUTOMATION_ID, COOLDOWN_SCHEME_VER, entity_hash)

    assert key == f"cooldown:{AUTOMATION_ID}:1:{entity_hash}"
    assert BASE_MESSAGE["alert"]["history_id"] not in key


# -- decision table ---------------------------------------------------------


def test_disabled_cooldown_proceeds_without_redis():
    redis = FakeRedis()
    msg = message()
    msg.cooldown = None

    decision = gate(redis).claim(msg, run_id=RUN_ID)

    assert decision.outcome is CooldownOutcome.DISABLED
    assert decision.should_submit is True
    assert redis.calls == []


def test_claim_stores_run_id_with_exact_provisional_ttl():
    redis = FakeRedis()

    decision = gate(redis).claim(message(), run_id=RUN_ID)

    assert decision.outcome is CooldownOutcome.CLAIMED
    assert decision.should_submit is True
    assert decision.owner_token == RUN_ID
    assert redis.values[decision.key] == RUN_ID
    assert redis.ttls[decision.key] == COOLDOWN_PROVISIONAL_TTL_SECONDS == 30


def test_nx_loss_suppresses_without_refreshing_winner_ttl():
    redis = FakeRedis()
    first = gate(redis).claim(message(), run_id=WINNER_RUN_ID)
    redis.ttls[first.key] = 17

    decision = gate(redis).claim(message(), run_id=LOSER_RUN_ID)

    assert decision.outcome is CooldownOutcome.SUPPRESSED
    assert decision.should_submit is False
    assert decision.entity_hash == first.entity_hash
    assert decision.eligible_again_in_seconds == 17
    assert decision.next_eligible_at == datetime.fromtimestamp(
        1_700_000_017, tz=timezone.utc
    )
    assert redis.values[first.key] == WINNER_RUN_ID
    assert redis.ttls[first.key] == 17


def test_missing_declared_field_proceeds_warns_and_never_touches_redis():
    redis = FakeRedis()
    alert = dict(BASE_MESSAGE["alert"])
    del alert["node_name"]

    decision = gate(redis).claim(message(alert=alert), run_id=RUN_ID)

    assert decision.outcome is CooldownOutcome.MISSING_FIELD
    assert decision.should_submit is True
    assert decision.missing_fields == ("node_name",)
    assert decision.gate_flags == {
        "cooldown": "skipped",
        "reason": "missing_field",
        "missing_fields": "node_name",
    }
    assert redis.calls == []


def test_empty_fields_claims_a_whole_automation_key():
    redis = FakeRedis()
    cooldown = {"fields": [], "seconds": 60, "scheme_ver": 1}

    decision = gate(redis).claim(message(cooldown=cooldown), run_id=RUN_ID)

    assert decision.outcome is CooldownOutcome.CLAIMED
    assert decision.entity_hash == canonical_cooldown_hash({}, [])


@pytest.mark.parametrize(
    "cooldown",
    [
        {"fields": ["site"], "seconds": 0, "scheme_ver": 1},
        {"fields": ["site"], "seconds": 300, "scheme_ver": 2},
        {"fields": ["site"], "seconds": 300, "scheme_ver": True},
        {"fields": "site", "seconds": 300, "scheme_ver": 1},
        {"fields": ["history_id"], "seconds": 300, "scheme_ver": 1},
    ],
)
def test_invalid_config_fails_open_without_redis(cooldown):
    redis = FakeRedis()

    decision = gate(redis).claim(message(cooldown=cooldown), run_id=RUN_ID)

    assert decision.outcome is CooldownOutcome.FAIL_OPEN
    assert decision.should_submit is True
    assert decision.gate_flags["reason"] == "invalid_config"
    assert redis.calls == []


def test_redis_claim_error_fails_open():
    redis = FakeRedis(fail_on={"set"})

    decision = gate(redis).claim(message(), run_id=RUN_ID)

    assert decision.outcome is CooldownOutcome.FAIL_OPEN
    assert decision.should_submit is True
    assert decision.gate_flags["reason"] == "redis_unavailable"


@pytest.mark.parametrize("run_id", ["", "not-a-uuid", None, 42])
def test_invalid_owner_token_fails_open_without_redis(run_id):
    redis = FakeRedis()

    decision = gate(redis).claim(message(), run_id=run_id)

    assert decision.outcome is CooldownOutcome.FAIL_OPEN
    assert decision.gate_flags["reason"] == "invalid_config"
    assert redis.calls == []


@pytest.mark.parametrize(
    "client_factory",
    [
        lambda: None,
        lambda: (_ for _ in ()).throw(ConnectionError("factory failed")),
    ],
)
def test_missing_or_raising_client_factory_fails_open(client_factory):
    decision = CooldownGate(client_factory).claim(message(), run_id=RUN_ID)

    assert decision.outcome is CooldownOutcome.FAIL_OPEN
    assert decision.should_submit is True
    assert decision.gate_flags["reason"] == "redis_unavailable"


def test_ttl_metadata_error_still_suppresses():
    redis = FakeRedis(fail_on={"ttl"})
    first = gate(redis).claim(message(), run_id=WINNER_RUN_ID)

    decision = gate(redis).claim(message(), run_id=LOSER_RUN_ID)

    assert first.outcome is CooldownOutcome.CLAIMED
    assert decision.outcome is CooldownOutcome.SUPPRESSED
    assert decision.eligible_again_in_seconds is None
    assert decision.next_eligible_at is None


# -- owned lifecycle --------------------------------------------------------


def test_activate_extends_full_ttl_only_for_owner():
    redis = FakeRedis()
    cooldown_gate = gate(redis)
    decision = cooldown_gate.claim(message(), run_id=RUN_ID)

    assert cooldown_gate.activate(decision) is True
    assert redis.values[decision.key] == RUN_ID
    assert redis.ttls[decision.key] == 300


def test_stale_owner_cannot_extend_replacement_claim():
    redis = FakeRedis()
    cooldown_gate = gate(redis)
    stale = cooldown_gate.claim(message(), run_id=STALE_RUN_ID)
    redis.values[stale.key] = REPLACEMENT_RUN_ID
    redis.ttls[stale.key] = 23

    assert cooldown_gate.activate(stale) is False
    assert redis.values[stale.key] == REPLACEMENT_RUN_ID
    assert redis.ttls[stale.key] == 23


def test_release_deletes_only_owned_provisional_claim():
    redis = FakeRedis()
    cooldown_gate = gate(redis)
    owned = cooldown_gate.claim(message(), run_id=RUN_ID)

    assert cooldown_gate.release(owned) is True
    assert owned.key not in redis.values


def test_stale_owner_cannot_delete_replacement_claim():
    redis = FakeRedis()
    cooldown_gate = gate(redis)
    stale = cooldown_gate.claim(message(), run_id=STALE_RUN_ID)
    redis.values[stale.key] = REPLACEMENT_RUN_ID

    assert cooldown_gate.release(stale) is False
    assert redis.values[stale.key] == REPLACEMENT_RUN_ID


@pytest.mark.parametrize("operation", ["activate", "release"])
def test_lifecycle_script_execution_failure_returns_false(operation):
    redis = FakeRedis(fail_on={operation})
    cooldown_gate = gate(redis)
    decision = cooldown_gate.claim(message(), run_id=RUN_ID)

    assert getattr(cooldown_gate, operation)(decision) is False
    assert redis.values[decision.key] == RUN_ID


@pytest.mark.parametrize("operation", ["activate", "release"])
def test_lifecycle_script_registration_failure_returns_false(operation):
    redis = FakeRedis(fail_on={"register_script"})
    cooldown_gate = gate(redis)
    decision = cooldown_gate.claim(message(), run_id=RUN_ID)

    assert getattr(cooldown_gate, operation)(decision) is False
    assert redis.values[decision.key] == RUN_ID


@pytest.mark.parametrize(
    "decision",
    [
        CooldownDecision(outcome=CooldownOutcome.DISABLED),
        CooldownDecision(outcome=CooldownOutcome.SUPPRESSED, key="key"),
        CooldownDecision(outcome=CooldownOutcome.FAIL_OPEN, key="key"),
    ],
)
def test_lifecycle_operations_ignore_non_owned_decisions(decision):
    redis = FakeRedis()
    cooldown_gate = gate(redis)

    assert cooldown_gate.activate(decision) is False
    assert cooldown_gate.release(decision) is False
    assert redis.calls == []


def test_registered_scripts_are_cached_for_shared_client():
    redis = FakeRedis()
    cooldown_gate = gate(redis)
    first = cooldown_gate.claim(message(), run_id=WINNER_RUN_ID)
    assert cooldown_gate.activate(first) is True

    redis.values[first.key] = REPLACEMENT_RUN_ID
    second = CooldownDecision(
        outcome=CooldownOutcome.CLAIMED,
        key=first.key,
        entity_hash=first.entity_hash,
        cooldown_seconds=first.cooldown_seconds,
        owner_token=REPLACEMENT_RUN_ID,
    )
    assert cooldown_gate.release(second) is True

    registrations = [call for call in redis.calls if call[0] == "register_script"]
    assert registrations == [
        ("register_script", "activate"),
        ("register_script", "release"),
    ]
