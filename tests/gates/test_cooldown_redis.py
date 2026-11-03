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
from src.bl.gates.cooldown_key import COOLDOWN_ARMED_VALUE
from src.bl.gates.cooldown_redis import (
    RECLAIM_CLAIMED,
    RECLAIM_LOST,
    RECLAIM_REENTERED,
    CooldownRedisLifecycle,
)
from src.models.cooldown import CooldownOutcome
from src.models.matched_message import MatchedAlertMessage
from src.models.submit import derive_run_id

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



def test_reclaim_re_enters_only_our_own_claim(redis_client):
    client, remember = redis_client
    automation_id = str(uuid4())
    gate = CooldownGate(client_factory=lambda: client)
    run_id, other_run_id = str(uuid4()), str(uuid4())

    first = gate.claim(message(automation_id), run_id=run_id)
    remember(first.key)
    client.expire(first.key, 3)

    again = gate.claim(message(automation_id), run_id=run_id)
    other = gate.claim(message(automation_id), run_id=other_run_id)

    assert again.outcome is CooldownOutcome.CLAIMED
    assert (first.reentered, again.reentered) == (False, True)
    assert 25 <= client.ttl(first.key) <= 30  # refreshed, not left at 3
    assert other.outcome is CooldownOutcome.SUPPRESSED
    assert client.get(first.key) == run_id


def test_reclaim_script_reports_reentered_claimed_and_lost(redis_client):
    client, remember = redis_client
    lifecycle = CooldownRedisLifecycle()
    key = remember(f"cooldown:{uuid4()}:1:reclaim-codes")
    run_id, other_run_id = str(uuid4()), str(uuid4())

    def reclaim(token):
        return lifecycle.reclaim(client, key=key, owner_token=token, ttl_seconds=30)

    assert reclaim(run_id) == RECLAIM_CLAIMED  # key absent: plain claim
    assert client.get(key) == run_id and 25 <= client.ttl(key) <= 30

    client.expire(key, 4)
    assert reclaim(run_id) == RECLAIM_REENTERED
    assert 25 <= client.ttl(key) <= 30

    client.expire(key, 4)
    assert reclaim(other_run_id) == RECLAIM_LOST
    assert client.get(key) == run_id and 0 < client.ttl(key) <= 4

    client.set(key, COOLDOWN_ARMED_VALUE, ex=60)
    assert reclaim(run_id) == RECLAIM_LOST  # armed retired the token
    assert client.get(key) == COOLDOWN_ARMED_VALUE and 55 <= client.ttl(key) <= 60


def test_a_dead_deliverys_claim_is_re_entered_by_its_event_only(redis_client):
    """A claim left by a consumer killed mid-submit: the redelivery of the same
    event derives the same run_id and re-enters it; another event loses."""
    client, remember = redis_client
    automation_id = str(uuid4())
    msg = message(automation_id)
    event_run_id = derive_run_id(automation_id, msg.history_id)
    gate = CooldownGate(lambda: client)

    dead = gate.claim(msg, run_id=event_run_id)  # the delivery that died here
    remember(dead.key)
    redelivery = gate.claim(msg, run_id=derive_run_id(automation_id, msg.history_id))
    other_event = gate.claim(
        msg, run_id=derive_run_id(automation_id, f"evt-{uuid4()}")
    )

    assert redelivery.outcome is CooldownOutcome.CLAIMED and redelivery.reentered
    assert other_event.outcome is CooldownOutcome.SUPPRESSED
    assert gate.activate(redelivery) is True
    assert client.get(dead.key) == COOLDOWN_ARMED_VALUE


def test_concurrent_deliveries_of_one_event_all_claim_others_lose(redis_client):
    client, remember = redis_client
    msg = message(str(uuid4()))
    gate = CooldownGate(lambda: client)
    run_id = str(uuid4())

    with ThreadPoolExecutor(max_workers=16) as pool:
        decisions = list(pool.map(lambda _: gate.claim(msg, run_id=run_id), range(16)))
    loser = gate.claim(msg, run_id=str(uuid4()))

    remember(decisions[0].key)
    assert {d.outcome for d in decisions} == {CooldownOutcome.CLAIMED}
    assert sum(not d.reentered for d in decisions) == 1  # exactly one fresh claim
    assert loser.outcome is CooldownOutcome.SUPPRESSED
    assert client.get(decisions[0].key) == run_id
