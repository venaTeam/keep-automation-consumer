"""Submit client: request shape (contracts §Submit) and response classification."""

import json

import httpx
import pytest

from src.bl.submit_client import SUBMIT_PATH, HttpSubmitClient, classify_response
from src.models.submit import SubmitOutcome, SubmitRequest, SubmitStatus

ALERT = {"history_id": "evt-789", "fingerprint": "abc123", "site": "us-east-1"}
REQUEST = SubmitRequest(
    tenant_id="tenant-a",
    automation_id="3f2b8c1e-4d6a-4b2f-9e77-0a1b2c3d4e5f",
    history_id="evt-789",
    fingerprint="abc123",
    run_id="7d4f2c60-1b8e-4c2a-9f3d-5e6a7b8c9d0e",
    alert=ALERT,
    gate_flags={"idempotency": "skipped", "reason": "redis_unavailable"},
)


def _client(handler, token="svc-token"):
    return HttpSubmitClient(
        base_url="http://automation-api.test",
        service_token=token,
        timeout_seconds=5.0,
        connect_timeout_seconds=1.0,
        max_connections=4,
        transport=httpx.MockTransport(handler),
    )


def test_request_carries_every_contract_field_and_the_bearer_token():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["path"] = request.url.path
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"status": "accepted", "run_id": REQUEST.run_id})

    result = _client(handler).submit(REQUEST)

    assert result.outcome is SubmitOutcome.ACCEPTED
    assert seen["method"] == "POST"
    assert seen["path"] == SUBMIT_PATH
    assert seen["auth"] == "Bearer svc-token"
    assert seen["body"] == {
        "tenant_id": "tenant-a",
        "automation_id": REQUEST.automation_id,
        "history_id": "evt-789",
        "fingerprint": "abc123",
        "run_id": REQUEST.run_id,
        "alert": ALERT,
        "gate_flags": {"idempotency": "skipped", "reason": "redis_unavailable"},
    }


def test_tenant_id_is_sent_verbatim():
    odd_tenant = "Tenant-ü/ 01"
    seen = {}

    def handler(request):
        seen["tenant_id"] = json.loads(request.content)["tenant_id"]
        return httpx.Response(200, json={"status": "accepted"})

    request = SubmitRequest(**{**REQUEST.__dict__, "tenant_id": odd_tenant})
    _client(handler).submit(request)

    assert seen["tenant_id"] == odd_tenant


def test_no_authorization_header_without_a_token():
    seen = {}

    def handler(request):
        seen["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={"status": "accepted"})

    _client(handler, token="").submit(REQUEST)

    assert seen["auth"] is None


@pytest.mark.parametrize(
    ("status", "outcome"),
    [
        ("accepted", SubmitOutcome.ACCEPTED),
        ("already_submitted", SubmitOutcome.ALREADY_SUBMITTED),
        ("suppressed", SubmitOutcome.SUPPRESSED),
    ],
)
def test_each_200_acknowledgement_is_acknowledged(status, outcome):
    body = json.dumps(
        {"status": status, "run_id": "existing-run", "suppression_reason": None}
    ).encode()

    result = classify_response(200, body)

    assert result.outcome is outcome
    assert result.acknowledged is True
    assert result.response.status is SubmitStatus(status)
    assert result.response.run_id == "existing-run"


@pytest.mark.parametrize(
    "body",
    [
        b'{"status": "ok"}',  # today's D17 stub
        b'{"status": "succeeded"}',  # a terminal status is not an acknowledgement
        b"{}",
        b"[]",
        b"<html>gateway</html>",
        b"",
    ],
)
def test_a_200_without_a_known_status_is_not_an_acknowledgement(body):
    result = classify_response(200, body)

    assert result.outcome is SubmitOutcome.INVALID_RESPONSE
    assert result.acknowledged is False


def test_capacity_429_is_its_own_outcome():
    result = classify_response(429, b'{"status": "capacity"}')

    assert result.outcome is SubmitOutcome.CAPACITY
    assert result.acknowledged is False


REFUSAL = b'{"status": "rejected", "reason": "tenant_mismatch"}'
VALIDATION_ERRORS = b'{"errors": [{"field": "history_id", "code": "required", "message": "x"}]}'


@pytest.mark.parametrize("code", [400, 404, 409, 422])
def test_typed_definitive_refusals_are_rejections(code):
    result = classify_response(code, REFUSAL)

    assert result.outcome is SubmitOutcome.REJECTED
    assert result.http_status == code


@pytest.mark.parametrize("code", [400, 404, 409, 422])
@pytest.mark.parametrize(
    "body",
    [
        b'{"detail": "Not Found"}',  # FastAPI default: a misrouted URL
        b'{"detail": "Automation not found"}',
        VALIDATION_ERRORS,  # generic validation: schema drift hits every message
        b"{}",
        b'{"errors": []}',
        b'{"errors": ["x"]}',
        b"<html>404</html>",
        b"",
    ],
)
def test_untyped_refusal_statuses_are_retried_not_skipped(code, body):
    """A wrong server answers every message like this; skipping on it would
    commit past the whole topic."""
    result = classify_response(code, body)

    assert result.outcome is SubmitOutcome.API_ERROR
    assert result.acknowledged is False


def test_proxy_environment_is_ignored():
    client = _client(lambda request: httpx.Response(200, json={"status": "accepted"}))

    assert client._client.trust_env is False


@pytest.mark.parametrize("code", [500, 502, 503, 504, 408, 401, 403, 307, 201, 413])
def test_everything_else_is_a_retryable_api_error(code):
    result = classify_response(code, b"{}")

    assert result.outcome is SubmitOutcome.API_ERROR
    assert result.acknowledged is False


def test_timeout_is_a_transport_error():
    def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    result = _client(handler).submit(REQUEST)

    assert result.outcome is SubmitOutcome.TRANSPORT_ERROR
    assert result.error.startswith("timeout")


def test_connection_failure_is_a_transport_error_without_the_token():
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    result = _client(handler, token="super-secret").submit(REQUEST)

    assert result.outcome is SubmitOutcome.TRANSPORT_ERROR
    assert "super-secret" not in (result.error or "")


def test_redirects_are_not_followed():
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(307, headers={"location": "http://elsewhere.test/x"})

    result = _client(handler).submit(REQUEST)

    assert calls == [SUBMIT_PATH]
    assert result.outcome is SubmitOutcome.API_ERROR
