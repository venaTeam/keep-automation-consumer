"""C11 integration: real Redis + the real HTTP submit client against a
contract-faithful fake API (unique `(history_id, automation_id)` → `200
already_submitted`, like D17's DB constraint).

CI supplies TEST_REDIS_URL. Local runs skip when Redis is not available; CI
must never silently skip this module.
"""

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from redis import Redis

from src.bl.gates.cooldown import CooldownGate
from src.bl.gates.cooldown_key import COOLDOWN_ARMED_VALUE
from src.bl.gates.idempotency import IdempotencyGate, idempotency_key
from src.bl.pipeline import SubmitPipeline
from src.bl.submit_client import HttpSubmitClient
from src.bl.suppression_audit import REASON_COOLDOWN, REASON_DUPLICATE
from src.config.consts import IDEMPOTENCY_TTL_SECONDS
from src.models.matched_message import MatchedAlertMessage
from src.models.submit import derive_run_id
from tests.helpers import RecordingAuditor

TEST_REDIS_URL = os.getenv("TEST_REDIS_URL")
if os.getenv("CI") and not TEST_REDIS_URL:
    raise RuntimeError("CI must set TEST_REDIS_URL for mandatory pipeline tests")

pytestmark = pytest.mark.skipif(
    not TEST_REDIS_URL,
    reason="TEST_REDIS_URL is required for real-Redis pipeline tests",
)


class FakeSubmitApi:
    """The submit contract's dedup semantics, nothing else."""

    def __init__(self, *, fail_with=None):
        self.rows = {}
        self.calls = 0
        self.fail_with = fail_with
        self._lock = threading.Lock()

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        with self._lock:
            self.calls += 1
            if self.fail_with is not None:
                return httpx.Response(self.fail_with, json={"status": "capacity"})
            key = (body["history_id"], body["automation_id"])
            if key in self.rows:
                return httpx.Response(
                    200, json={"status": "already_submitted", "run_id": self.rows[key]}
                )
            self.rows[key] = body["run_id"]
            return httpx.Response(200, json={"status": "accepted", "run_id": body["run_id"]})


@pytest.fixture
def redis_client():
    client = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    client.ping()
    automation_id = str(uuid4())
    yield client, automation_id
    keys = list(client.scan_iter(f"*{automation_id}*"))
    if keys:
        client.delete(*keys)
    client.close()


def pipeline_for(client, api, auditor=None):
    submit_client = HttpSubmitClient(
        base_url="http://automation-api.test",
        service_token="t",
        timeout_seconds=5.0,
        connect_timeout_seconds=1.0,
        transport=httpx.MockTransport(api.handler),
    )
    return SubmitPipeline(
        idempotency_gate=IdempotencyGate(client_factory=lambda: client),
        cooldown_gate=CooldownGate(client_factory=lambda: client),
        submit_client=submit_client,
        suppression_auditor=auditor or RecordingAuditor(),
        backoff_initial_seconds=0.0,
        backoff_max_seconds=0.0,
    )


def message(automation_id, *, history_id=None, cooldown_seconds=300):
    return MatchedAlertMessage.from_bytes(
        json.dumps(
            {
                "tenant_id": "keep",
                "alert": {
                    "site": "us-east-1",
                    "node_name": "api-01",
                    "fingerprint": "abc123",
                    "history_id": history_id or f"evt-{uuid4()}",
                },
                "automation_id": automation_id,
                "matched_m": 1,
                "cooldown": {
                    "fields": ["site", "node_name"],
                    "seconds": cooldown_seconds,
                    "scheme_ver": 1,
                },
            }
        ).encode()
    )


def cooldown_keys(client, automation_id):
    return list(client.scan_iter(f"cooldown:{automation_id}:*"))


def test_redelivery_is_one_submit_and_one_audited_duplicate(redis_client):
    client, automation_id = redis_client
    api, auditor = FakeSubmitApi(), RecordingAuditor()
    pipeline = pipeline_for(client, api, auditor)
    msg = message(automation_id)

    first = pipeline.run(msg, cancelled=threading.Event())
    second = pipeline.run(msg, cancelled=threading.Event())

    assert (first.outcome, second.outcome) == ("accepted", "duplicate")
    assert api.calls == 1
    assert [row["reason"] for row in auditor.rows] == [REASON_DUPLICATE]
    key = idempotency_key(msg.history_id, automation_id)
    assert client.get(key) == "done"
    assert IDEMPOTENCY_TTL_SECONDS - 5 <= client.ttl(key) <= IDEMPOTENCY_TTL_SECONDS


def test_accepted_arms_the_full_cooldown_and_suppresses_the_next_event(redis_client):
    client, automation_id = redis_client
    api, auditor = FakeSubmitApi(), RecordingAuditor()
    pipeline = pipeline_for(client, api, auditor)

    first = pipeline.run(message(automation_id), cancelled=threading.Event())
    second = pipeline.run(message(automation_id), cancelled=threading.Event())

    assert (first.outcome, second.outcome) == ("accepted", "cooldown_suppressed")
    (key,) = cooldown_keys(client, automation_id)
    assert client.get(key) == COOLDOWN_ARMED_VALUE
    assert 290 <= client.ttl(key) <= 300
    assert auditor.rows[-1]["reason"] == REASON_COOLDOWN
    assert api.calls == 1


def test_capacity_releases_the_provisional_claim(redis_client):
    client, automation_id = redis_client
    api = FakeSubmitApi(fail_with=429)
    pipeline = pipeline_for(client, api)
    msg = message(automation_id)

    result = pipeline.process_once(msg, run_id=str(uuid4()))

    assert result.settled is False
    assert cooldown_keys(client, automation_id) == []
    assert client.get(idempotency_key(msg.history_id, automation_id)) == "pending"


class ProcessKilled(BaseException):
    """Stands in for SIGKILL/OOM: nothing after it runs in the dying consumer."""


@pytest.mark.parametrize("reached_api", [False, True])
def test_redelivery_after_a_crash_mid_submit_runs_and_arms(redis_client, reached_api):
    """VAULT C11 follow-up 1, end to end: a consumer killed mid-submit leaves
    idem `pending` and a provisional claim under the event's run_id. The
    redelivery — well inside the 30s, as with a short `session.timeout.ms` —
    must re-enter that claim, not be suppressed by it."""
    client, automation_id = redis_client
    api = FakeSubmitApi()
    msg = message(automation_id)
    left = {}

    def killed_mid_submit(request):
        # What Redis holds at the moment of the kill. The dying process's
        # unwind `release` below never runs under a real SIGKILL, so it is
        # undone afterwards.
        for key in [*cooldown_keys(client, automation_id),
                    idempotency_key(msg.history_id, automation_id)]:
            left[key] = (client.get(key), client.pttl(key))
        if reached_api:
            api.handler(request)  # the API accepted it; the reply never came back
        raise ProcessKilled

    dying = pipeline_for(client, SimpleNamespace(handler=killed_mid_submit))
    with pytest.raises(ProcessKilled):
        dying.run(msg, cancelled=threading.Event())
    for key, (value, pttl) in left.items():
        client.set(key, value, px=pttl)
    (claim_key,) = cooldown_keys(client, automation_id)
    assert client.get(claim_key) == derive_run_id(automation_id, msg.history_id)

    auditor = RecordingAuditor()
    result = pipeline_for(client, api, auditor).run(msg, cancelled=threading.Event())

    assert result.outcome == ("already_submitted" if reached_api else "accepted")
    assert auditor.rows == []
    assert len(api.rows) == 1
    assert client.get(claim_key) == COOLDOWN_ARMED_VALUE
    assert 290 <= client.ttl(claim_key) <= 300
    assert client.get(idempotency_key(msg.history_id, automation_id)) == "done"


def test_concurrent_redeliveries_converge_to_exactly_one_accepted_run(redis_client):
    """The story's chaos property at the pipeline level: however many copies of
    one event race through the gates, the API accepts it exactly once."""
    client, automation_id = redis_client
    api = FakeSubmitApi()
    pipeline = pipeline_for(client, api)
    msg = message(automation_id)

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(
            pool.map(lambda _: pipeline.run(msg, cancelled=threading.Event()), range(32))
        )

    outcomes = [r.outcome for r in results]
    assert outcomes.count("accepted") == 1
    # The consumer's own state: the event is marked done, and its cooldown is
    # either armed for the full window or — when a sibling that saw a stale-
    # looking already_submitted released the shared provisional claim before
    # the winner armed it (contracts §Redis keys, accepted trade-off) — absent.
    # Never a provisional claim left suppressing the entity.
    assert client.get(idempotency_key(msg.history_id, automation_id)) == "done"
    keys = cooldown_keys(client, automation_id)
    assert len(keys) <= 1
    for key in keys:
        assert client.get(key) == COOLDOWN_ARMED_VALUE
        assert 290 <= client.ttl(key) <= 300
    assert set(outcomes) <= {
        "accepted",
        "already_submitted",
        "duplicate",
        "cooldown_suppressed",
    }
    assert len(api.rows) == 1
