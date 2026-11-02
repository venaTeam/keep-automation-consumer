"""C11 pipeline: spec §5.2's sequence and failure table, row by row.

Gates are the real C9/C10 implementations over an in-memory Redis; only the
API is scripted. Each test names the table row it pins.
"""

import json
import logging
import threading
from uuid import UUID

import pytest
from prometheus_client import REGISTRY

from src.bl.gates.cooldown import CooldownGate
from src.bl.gates.cooldown_key import (
    COOLDOWN_ARMED_VALUE,
    COOLDOWN_PROVISIONAL_TTL_SECONDS,
    canonical_cooldown_hash,
    cooldown_key,
)
from src.bl.gates.idempotency import IdempotencyGate, idempotency_key
from src.bl.suppression_audit import REASON_COOLDOWN, REASON_DUPLICATE
from src.config.consts import IDEMPOTENCY_TTL_SECONDS
from src.models.matched_message import MatchedAlertMessage
from src.models.submit import SubmitOutcome, SubmitStatus
from tests.helpers import (
    ACCEPTED,
    InMemoryRedis,
    RecordingAuditor,
    ScriptedSubmitClient,
    acknowledged,
    build_test_pipeline,
    failure,
)

AUTOMATION_ID = "3f2b8c1e-4d6a-4b2f-9e77-0a1b2c3d4e5f"
HISTORY_ID = "evt-789"
OTHER_RUN_ID = "c486bfc1-dde9-4354-bde4-a7a3164498a0"

MESSAGE = {
    "tenant_id": "tenant-a",
    "alert": {
        "application": "payments",
        "severity": "critical",
        "status": "firing",
        "component": "api",
        "site": "us-east-1",
        "environment": "prod",
        "operator": "team-payments",
        "node_name": "api-01",
        "fingerprint": "abc123",
        "history_id": HISTORY_ID,
        "time_created": "2026-07-12T14:03:00Z",
    },
    "automation_id": AUTOMATION_ID,
    "matched_m": 2,
    "cooldown": {"fields": ["site", "node_name"], "seconds": 300, "scheme_ver": 1},
}

IDEM_KEY = idempotency_key(HISTORY_ID, AUTOMATION_ID)
COOLDOWN_KEY = cooldown_key(
    AUTOMATION_ID, 1, canonical_cooldown_hash(MESSAGE["alert"], ["site", "node_name"])
)
RUN_ID = "7d4f2c60-1b8e-4c2a-9f3d-5e6a7b8c9d0e"


def message(**overrides) -> MatchedAlertMessage:
    payload = json.loads(json.dumps(MESSAGE))
    payload.update(overrides)
    return MatchedAlertMessage.from_bytes(json.dumps(payload).encode())


def counter(name, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def submits(outcome) -> float:
    return counter("keep_automation_consumer_submits_total", outcome=outcome)


# -- 200 acknowledgements -----------------------------------------------------


def test_accepted_marks_done_arms_the_owned_cooldown_and_settles():
    redis = InMemoryRedis()
    seen = {}

    def inspect(request):
        # Mid-flight: the provisional claim is owned by this request's run_id.
        seen["claim_value"] = redis.values.get(COOLDOWN_KEY)
        seen["claim_ttl"] = redis.ttls.get(COOLDOWN_KEY)
        seen["idem"] = redis.values.get(IDEM_KEY)

    client = ScriptedSubmitClient(ACCEPTED, on_submit=inspect)
    pipeline = build_test_pipeline(redis=redis, submit_client=client)

    result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is True
    assert result.outcome == "accepted"
    assert seen == {
        "claim_value": RUN_ID,
        "claim_ttl": COOLDOWN_PROVISIONAL_TTL_SECONDS,
        "idem": "pending",
    }
    assert redis.values[IDEM_KEY] == "done"
    assert redis.ttls[IDEM_KEY] == IDEMPOTENCY_TTL_SECONDS
    assert redis.values[COOLDOWN_KEY] == COOLDOWN_ARMED_VALUE
    assert redis.ttls[COOLDOWN_KEY] == 300


def test_request_carries_tenant_verbatim_run_id_and_gate_flags():
    redis = InMemoryRedis()
    client = ScriptedSubmitClient(ACCEPTED)
    pipeline = build_test_pipeline(redis=redis, submit_client=client)

    pipeline.process_once(message(tenant_id="Tenant-ü 01"), run_id=RUN_ID)

    (request,) = client.requests
    assert request.tenant_id == "Tenant-ü 01"
    assert request.automation_id == AUTOMATION_ID
    assert request.history_id == HISTORY_ID
    assert request.fingerprint == "abc123"
    assert request.run_id == RUN_ID
    assert request.alert == MESSAGE["alert"]
    assert request.gate_flags == {"idempotency": "claimed", "cooldown": "claimed"}


@pytest.mark.parametrize(
    "status", [SubmitStatus.ALREADY_SUBMITTED, SubmitStatus.SUPPRESSED]
)
def test_other_200s_mark_done_release_the_claim_and_settle(status):
    redis = InMemoryRedis()
    pipeline = build_test_pipeline(
        redis=redis, submit_client=ScriptedSubmitClient(acknowledged(status))
    )

    result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is True
    assert result.outcome == status.value
    assert redis.values[IDEM_KEY] == "done"
    assert COOLDOWN_KEY not in redis.values


def test_accepted_after_losing_the_claim_settles_without_touching_the_new_owner(
    caplog,
):
    """Stale owner: the claim expired mid-submit and another event re-claimed
    it. Compare-and-extend must not arm someone else's key."""
    redis = InMemoryRedis()

    def steal_claim(_request):
        redis.values[COOLDOWN_KEY] = OTHER_RUN_ID
        redis.ttls[COOLDOWN_KEY] = COOLDOWN_PROVISIONAL_TTL_SECONDS

    pipeline = build_test_pipeline(
        redis=redis, submit_client=ScriptedSubmitClient(ACCEPTED, on_submit=steal_claim)
    )

    with caplog.at_level(logging.WARNING, logger="src.bl.pipeline"):
        result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is True
    assert redis.values[COOLDOWN_KEY] == OTHER_RUN_ID
    assert redis.ttls[COOLDOWN_KEY] == COOLDOWN_PROVISIONAL_TTL_SECONDS
    assert "could not arm its cooldown" in caplog.text


# -- unacknowledged -----------------------------------------------------------


def test_capacity_429_releases_the_claim_leaves_idem_pending_and_retries():
    redis = InMemoryRedis()
    before = counter("keep_automation_consumer_submit_capacity_backoffs_total")
    pipeline = build_test_pipeline(
        redis=redis,
        submit_client=ScriptedSubmitClient(failure(SubmitOutcome.CAPACITY, 429)),
    )

    result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is False
    assert result.outcome == "capacity"
    assert COOLDOWN_KEY not in redis.values
    assert redis.values[IDEM_KEY] == "pending"
    assert counter("keep_automation_consumer_submit_capacity_backoffs_total") == before + 1


@pytest.mark.parametrize(
    "outcome",
    [
        SubmitOutcome.API_ERROR,
        SubmitOutcome.TRANSPORT_ERROR,
        SubmitOutcome.INVALID_RESPONSE,
    ],
)
def test_clean_api_failures_release_the_claim_and_retry(outcome):
    redis = InMemoryRedis()
    before = submits(outcome.value)
    pipeline = build_test_pipeline(
        redis=redis, submit_client=ScriptedSubmitClient(failure(outcome, 503))
    )

    result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is False
    assert COOLDOWN_KEY not in redis.values
    assert redis.values[IDEM_KEY] == "pending"
    assert submits(outcome.value) == before + 1


def test_a_failure_never_deletes_a_claim_it_does_not_own():
    redis = InMemoryRedis()

    def steal_claim(_request):
        redis.values[COOLDOWN_KEY] = OTHER_RUN_ID

    pipeline = build_test_pipeline(
        redis=redis,
        submit_client=ScriptedSubmitClient(
            failure(SubmitOutcome.CAPACITY, 429), on_submit=steal_claim
        ),
    )

    pipeline.process_once(message(), run_id=RUN_ID)

    assert redis.values[COOLDOWN_KEY] == OTHER_RUN_ID


@pytest.mark.parametrize("status", [400, 404, 409, 422])
def test_definitive_refusal_releases_the_claim_and_settles_loudly(status, caplog):
    redis = InMemoryRedis()
    before = counter("keep_automation_consumer_submit_rejected_total", status=str(status))
    pipeline = build_test_pipeline(
        redis=redis,
        submit_client=ScriptedSubmitClient(failure(SubmitOutcome.REJECTED, status)),
    )

    with caplog.at_level(logging.ERROR, logger="src.bl.pipeline"):
        result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is True
    assert result.outcome == "rejected"
    assert COOLDOWN_KEY not in redis.values
    assert redis.values[IDEM_KEY] == "pending"  # not an acknowledgement
    assert (
        counter("keep_automation_consumer_submit_rejected_total", status=str(status))
        == before + 1
    )
    assert "will NOT run" in caplog.text
    assert AUTOMATION_ID in caplog.text and HISTORY_ID in caplog.text


# -- gates suppress -----------------------------------------------------------


def test_duplicate_is_audited_settled_and_never_submitted():
    redis = InMemoryRedis()
    redis.values[IDEM_KEY] = "done"
    auditor = RecordingAuditor()
    client = ScriptedSubmitClient()
    pipeline = build_test_pipeline(
        redis=redis, submit_client=client, suppression_auditor=auditor
    )

    result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is True
    assert result.outcome == "duplicate"
    assert client.requests == []
    assert [row["reason"] for row in auditor.rows] == [REASON_DUPLICATE]
    assert auditor.rows[0]["tenant_id"] == "tenant-a"


def test_cooldown_suppression_is_audited_settled_and_leaves_the_winner_alone():
    redis = InMemoryRedis()
    redis.values[COOLDOWN_KEY] = COOLDOWN_ARMED_VALUE
    redis.ttls[COOLDOWN_KEY] = 120
    auditor = RecordingAuditor()
    client = ScriptedSubmitClient()
    pipeline = build_test_pipeline(
        redis=redis, submit_client=client, suppression_auditor=auditor
    )

    result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is True
    assert result.outcome == "cooldown_suppressed"
    assert client.requests == []
    assert redis.values[COOLDOWN_KEY] == COOLDOWN_ARMED_VALUE
    assert redis.ttls[COOLDOWN_KEY] == 120
    (row,) = auditor.rows
    assert row["reason"] == REASON_COOLDOWN
    assert row["detail"]["entity_hash"] == COOLDOWN_KEY.rsplit(":", 1)[1]
    assert row["detail"]["eligible_again_in_seconds"] == 120
    assert row["detail"]["next_eligible_at"] is not None
    # Suppression is not an acknowledgement: the idem key is not `done`.
    assert redis.values[IDEM_KEY] == "pending"


def test_a_failing_auditor_does_not_fail_the_message(caplog):
    class AngryAuditor:
        def record_suppression(self, **_):
            raise RuntimeError("audit API 503")

    redis = InMemoryRedis()
    redis.values[IDEM_KEY] = "done"
    pipeline = build_test_pipeline(redis=redis, suppression_auditor=AngryAuditor())

    with caplog.at_level(logging.ERROR, logger="src.bl.pipeline"):
        result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is True
    assert "could not audit a duplicate suppression" in caplog.text


# -- fail-open ----------------------------------------------------------------


def test_redis_down_submits_without_gates_and_flags_it():
    """Spec §6.3: submit without gates, audited via gate_flags, paged."""
    idem_redis = InMemoryRedis(fail_on={"*"})
    cooldown_redis = InMemoryRedis()
    client = ScriptedSubmitClient(ACCEPTED)
    pipeline = build_test_pipeline(
        idempotency_gate=IdempotencyGate(client_factory=lambda: idem_redis),
        cooldown_gate=CooldownGate(client_factory=lambda: cooldown_redis),
        submit_client=client,
    )
    before = counter(
        "keep_automation_consumer_fail_open_submits_total", reason="redis_unavailable"
    )

    result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is True
    (request,) = client.requests
    assert request.gate_flags == {
        "idempotency": "skipped",
        "reason": "redis_unavailable",
        "cooldown": "skipped",
        "cooldown_reason": "redis_unavailable",
    }
    assert cooldown_redis.calls == []  # no second timeout per message
    assert (
        counter(
            "keep_automation_consumer_fail_open_submits_total",
            reason="redis_unavailable",
        )
        == before + 1
    )


def test_redis_unset_submits_without_gates():
    client = ScriptedSubmitClient(ACCEPTED)
    pipeline = build_test_pipeline(redis=None, submit_client=client)

    result = pipeline.process_once(message(), run_id=RUN_ID)

    assert result.settled is True
    assert client.requests[0].gate_flags["idempotency"] == "skipped"
    assert client.requests[0].gate_flags["cooldown"] == "skipped"


def test_missing_history_id_fails_open_but_cooldown_still_runs():
    redis = InMemoryRedis()
    alert = {k: v for k, v in MESSAGE["alert"].items() if k != "history_id"}
    client = ScriptedSubmitClient(ACCEPTED)
    pipeline = build_test_pipeline(redis=redis, submit_client=client)
    before = counter(
        "keep_automation_consumer_fail_open_submits_total", reason="missing_history_id"
    )

    pipeline.process_once(message(alert=alert), run_id=RUN_ID)

    flags = client.requests[0].gate_flags
    assert flags["reason"] == "missing_history_id"
    assert flags["cooldown"] == "claimed"
    assert redis.values[COOLDOWN_KEY] == COOLDOWN_ARMED_VALUE
    assert (
        counter(
            "keep_automation_consumer_fail_open_submits_total",
            reason="missing_history_id",
        )
        == before + 1
    )


def test_missing_cooldown_field_runs_with_a_warning_and_arms_nothing():
    redis = InMemoryRedis()
    alert = {k: v for k, v in MESSAGE["alert"].items() if k != "node_name"}
    client = ScriptedSubmitClient(ACCEPTED)
    pipeline = build_test_pipeline(redis=redis, submit_client=client)

    result = pipeline.process_once(message(alert=alert), run_id=RUN_ID)

    assert result.settled is True
    assert client.requests[0].gate_flags["cooldown_reason"] == "missing_field"
    assert client.requests[0].gate_flags["cooldown_missing_fields"] == "node_name"
    assert not any(key.startswith("cooldown:") for key in redis.values)


def test_cooldown_off_submits_with_cooldown_disabled():
    redis = InMemoryRedis()
    client = ScriptedSubmitClient(ACCEPTED)
    pipeline = build_test_pipeline(redis=redis, submit_client=client)

    pipeline.process_once(message(cooldown=None), run_id=RUN_ID)

    assert client.requests[0].gate_flags == {
        "idempotency": "claimed",
        "cooldown": "disabled",
    }


# -- retry loop ---------------------------------------------------------------


def test_run_retries_with_the_same_run_id_until_acknowledged():
    redis = InMemoryRedis()
    claims = []

    def record_claim(_request):
        claims.append(redis.values.get(COOLDOWN_KEY))

    client = ScriptedSubmitClient(
        failure(SubmitOutcome.CAPACITY, 429),
        failure(SubmitOutcome.API_ERROR, 503),
        ACCEPTED,
        on_submit=record_claim,
    )
    pipeline = build_test_pipeline(redis=redis, submit_client=client)

    result = pipeline.run(message(), cancelled=threading.Event())

    assert result.outcome == "accepted"
    run_ids = {request.run_id for request in client.requests}
    assert len(client.requests) == 3 and len(run_ids) == 1
    UUID(run_ids.pop())  # minted per delivered message, a real uuid
    # Each attempt re-won the provisional claim it released on failure.
    assert claims == [client.requests[0].run_id] * 3
    assert redis.values[COOLDOWN_KEY] == COOLDOWN_ARMED_VALUE
    # The second attempt was "ambiguous" (key still pending), not a duplicate.
    assert client.requests[1].gate_flags["idempotency"] == "ambiguous"


def test_each_delivery_mints_a_new_run_id():
    client = ScriptedSubmitClient(ACCEPTED)
    pipeline = build_test_pipeline(redis=InMemoryRedis(), submit_client=client)

    pipeline.run(message(), cancelled=threading.Event())
    other_event = {**MESSAGE["alert"], "history_id": "evt-other", "site": "eu-west-1"}
    pipeline.run(message(alert=other_event), cancelled=threading.Event())

    assert client.requests[0].run_id != client.requests[1].run_id


def test_shutdown_during_backoff_abandons_without_another_attempt():
    cancelled = threading.Event()
    client = ScriptedSubmitClient(
        failure(SubmitOutcome.TRANSPORT_ERROR),
        on_submit=lambda _request: cancelled.set(),
    )
    pipeline = build_test_pipeline(
        redis=InMemoryRedis(),
        submit_client=client,
        backoff_initial_seconds=60.0,
        backoff_max_seconds=60.0,
    )
    before = counter(
        "keep_automation_consumer_messages_abandoned_total", reason="shutdown"
    )

    result = pipeline.run(message(), cancelled=cancelled)  # must not sleep 60s

    assert result is None
    assert len(client.requests) == 1
    assert (
        counter("keep_automation_consumer_messages_abandoned_total", reason="shutdown")
        == before + 1
    )


def test_losing_the_partition_abandons_the_retry():
    owned = {"value": True}
    client = ScriptedSubmitClient(
        failure(SubmitOutcome.API_ERROR, 503),
        on_submit=lambda _request: owned.update(value=False),
    )
    pipeline = build_test_pipeline(redis=InMemoryRedis(), submit_client=client)

    result = pipeline.run(
        message(), cancelled=threading.Event(), still_owned=lambda: owned["value"]
    )

    assert result is None
    assert len(client.requests) == 1


def test_already_cancelled_message_is_not_attempted():
    cancelled = threading.Event()
    cancelled.set()
    client = ScriptedSubmitClient()
    pipeline = build_test_pipeline(redis=InMemoryRedis(), submit_client=client)

    assert pipeline.run(message(), cancelled=cancelled) is None
    assert client.requests == []


def test_backoff_is_capped_exponential_with_equal_jitter():
    pipeline = build_test_pipeline(backoff_initial_seconds=1.0, backoff_max_seconds=30.0)

    pipeline._random_fraction = lambda: 0.0
    floors = [pipeline._backoff_delay(attempt) for attempt in range(1, 8)]
    pipeline._random_fraction = lambda: 1.0
    ceilings = [pipeline._backoff_delay(attempt) for attempt in range(1, 8)]

    assert floors == [0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 15.0]
    assert ceilings == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]
    assert pipeline._backoff_delay(10_000) == 30.0  # no overflow on long outages


def test_redelivery_after_acknowledgement_is_one_submit_and_one_audited_duplicate():
    """The C9 half of the story's chaos check, through the whole pipeline."""
    redis = InMemoryRedis()
    auditor = RecordingAuditor()
    client = ScriptedSubmitClient(ACCEPTED)
    pipeline = build_test_pipeline(
        redis=redis, submit_client=client, suppression_auditor=auditor
    )

    first = pipeline.run(message(), cancelled=threading.Event())
    second = pipeline.run(message(), cancelled=threading.Event())

    assert (first.outcome, second.outcome) == ("accepted", "duplicate")
    assert len(client.requests) == 1
    assert [row["reason"] for row in auditor.rows] == [REASON_DUPLICATE]
