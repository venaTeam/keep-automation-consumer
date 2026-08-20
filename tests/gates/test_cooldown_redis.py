"""Real-Redis ownership and atomicity tests for C10.

CI supplies TEST_REDIS_URL. Local runs skip when Redis is not available; CI
must never silently skip this module.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from redis import Redis

from src.bl.gates.cooldown import CooldownGate
from src.contracts.cooldown import COOLDOWN_ARMED_VALUE
from src.models.cooldown import CooldownOutcome
from src.models.matched_message import MatchedAlertMessage

TEST_REDIS_URL = os.getenv("TEST_REDIS_URL")
if os.getenv("CI") and not TEST_REDIS_URL:
    raise RuntimeError("CI must set TEST_REDIS_URL for mandatory cooldown Lua tests")

pytestmark = pytest.mark.skipif(
    not TEST_REDIS_URL,
    reason="TEST_REDIS_URL is required for real-Redis cooldown tests",
)


@pytest.fixture
def redis_client():
    client = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    client.ping()
    keys = []

    def remember(key):
        keys.append(key)
        return key

    yield client, remember
    if keys:
        client.delete(*keys)
    client.close()


def message(automation_id: str, *, seconds: int = 120) -> MatchedAlertMessage:
    payload = {
        "tenant_id": "keep",
        "alert": {
            "application": "payments",
            "component": "api",
            "status": "firing",
            "site": "us-east-1",
            "severity": "critical",
            "environment": "prod",
            "operator": "team-payments",
            "fingerprint": "abc123",
            "history_id": f"evt-{uuid4()}",
            "time_created": "2026-07-12T14:03:00Z",
        },
        "automation_id": automation_id,
        "matched_m": 1,
        "cooldown": {
            "fields": ["site"],
            "seconds": seconds,
            "scheme_ver": 1,
        },
    }
    return MatchedAlertMessage.from_bytes(json.dumps(payload).encode())


def test_concurrent_claim_has_one_winner_and_never_refreshes_on_loss(redis_client):
    client, remember = redis_client
    msg = message(str(uuid4()))
    gate = CooldownGate(lambda: client)

    with ThreadPoolExecutor(max_workers=16) as pool:
        decisions = list(
            pool.map(
                lambda token: gate.claim(msg, run_id=token),
                [str(uuid4()) for _ in range(16)],
            )
        )

    claimed = [d for d in decisions if d.outcome is CooldownOutcome.CLAIMED]
    suppressed = [d for d in decisions if d.outcome is CooldownOutcome.SUPPRESSED]
    assert len(claimed) == 1
    assert len(suppressed) == 15
    key = remember(claimed[0].key)
    assert client.get(key) == claimed[0].owner_token
    assert 0 < client.ttl(key) <= 30

    client.expire(key, 5)
    loser = gate.claim(msg, run_id=str(uuid4()))
    assert loser.outcome is CooldownOutcome.SUPPRESSED
    assert 0 < client.ttl(key) <= 5


def test_compare_and_extend_requires_current_owner(redis_client):
    client, remember = redis_client
    msg = message(str(uuid4()), seconds=180)
    gate = CooldownGate(lambda: client)
    decision = gate.claim(msg, run_id=str(uuid4()))
    remember(decision.key)

    assert gate.activate(decision) is True
    assert client.get(decision.key) == COOLDOWN_ARMED_VALUE
    assert 0 < client.ttl(decision.key) <= 180

    # Arming retired the run_id token: even the owner can no longer extend
    # or delete the claim (release here would be a C11 error-path bug).
    assert gate.activate(decision) is False
    assert gate.release(decision) is False
    assert client.get(decision.key) == COOLDOWN_ARMED_VALUE
    assert 0 < client.ttl(decision.key) <= 180

    client.set(decision.key, "replacement", ex=23)
    assert gate.activate(decision) is False
    assert client.get(decision.key) == "replacement"
    assert 0 < client.ttl(decision.key) <= 23


def test_compare_and_delete_requires_current_owner(redis_client):
    client, remember = redis_client
    msg = message(str(uuid4()))
    gate = CooldownGate(lambda: client)
    decision = gate.claim(msg, run_id=str(uuid4()))
    remember(decision.key)

    client.set(decision.key, "replacement", ex=30)
    assert gate.release(decision) is False
    assert client.get(decision.key) == "replacement"

    replacement = gate.claim(msg, run_id=str(uuid4()))
    assert replacement.outcome is CooldownOutcome.SUPPRESSED
    client.set(decision.key, decision.owner_token, ex=30)
    assert gate.release(decision) is True
    assert client.exists(decision.key) == 0
