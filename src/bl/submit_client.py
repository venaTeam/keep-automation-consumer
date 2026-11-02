"""Client for the automation API's `POST /internal/submit` (contracts §Submit).

The endpoint acknowledges early: a 200 means the run row is durable as
`submitted` (or was already there, or the automation is no longer active) — it
never carries the `/run` outcome. Every response is classified into a
`SubmitOutcome` here, so the pipeline never inspects HTTP details.

Nothing here raises for an HTTP or transport failure: a submit that could not be
acknowledged is a classified result, and the pipeline decides between retry and
skip. The service token is sent as a bearer header and never logged — errors
carry only an exception class name or a status code.
"""

import json
import logging
from typing import Optional, Protocol

import httpx

from src.models.submit import (
    SubmitOutcome,
    SubmitRequest,
    SubmitResponse,
    SubmitResult,
    SubmitStatus,
)

logger = logging.getLogger(__name__)

SUBMIT_PATH = "/internal/submit"

# The API refused this message for good (spec §8.4, contracts §Submit): bad
# request, unknown automation, tenant mismatch, validation. Retrying cannot
# succeed, so the pipeline skips it — but only when the body proves it is the
# submit endpoint refusing (see `_is_refusal_body`). Everything else is
# retryable, including 401/403 (a token misconfiguration recovers once fixed)
# and 413 (an ingress body limit recovers once raised).
DEFINITIVE_REFUSAL_STATUSES = frozenset({400, 404, 409, 422})
REFUSAL_STATUS = "rejected"
CAPACITY_STATUS = 429

_ACKNOWLEDGED = {
    SubmitStatus.ACCEPTED: SubmitOutcome.ACCEPTED,
    SubmitStatus.ALREADY_SUBMITTED: SubmitOutcome.ALREADY_SUBMITTED,
    SubmitStatus.SUPPRESSED: SubmitOutcome.SUPPRESSED,
}


class SubmitClient(Protocol):
    def submit(self, request: SubmitRequest) -> SubmitResult: ...

    def close(self) -> None: ...


class HttpSubmitClient:
    """One process-lifetime `httpx.Client` shared by every worker.

    `httpx.Client` is thread-safe and pools connections; building one per
    message would pay a TCP (and TLS) handshake per submit at ~200/s. The pool is
    sized to the worker pool so a worker never waits on a connection slot.
    Redirects are not followed and proxy env vars are ignored: either would be
    a routing mistake that could carry the token elsewhere.
    """

    def __init__(
        self,
        *,
        base_url: str,
        service_token: str,
        timeout_seconds: float,
        connect_timeout_seconds: float,
        max_connections: int,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        headers = {"Accept": "application/json"}
        if service_token:
            headers["Authorization"] = f"Bearer {service_token}"
        self._client = httpx.Client(
            base_url=base_url,
            headers=headers,
            timeout=httpx.Timeout(timeout_seconds, connect=connect_timeout_seconds),
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            ),
            follow_redirects=False,
            # The target is always the internal API: never route the token and
            # payloads through a proxy picked up from HTTP(S)_PROXY.
            trust_env=False,
            transport=transport,
        )

    def submit(self, request: SubmitRequest) -> SubmitResult:
        try:
            response = self._client.post(SUBMIT_PATH, json=request.to_dict())
        except httpx.TimeoutException as exc:
            return SubmitResult(
                SubmitOutcome.TRANSPORT_ERROR, error=f"timeout:{type(exc).__name__}"
            )
        except httpx.HTTPError as exc:
            return SubmitResult(SubmitOutcome.TRANSPORT_ERROR, error=type(exc).__name__)
        return classify_response(response.status_code, response.content)

    def close(self) -> None:
        self._client.close()


def classify_response(status_code: int, body: bytes) -> SubmitResult:
    if status_code == 200:
        response = _parse_acknowledgement(body)
        if response is None:
            # A 200 that is not a contract acknowledgement (a proxy's HTML page,
            # a stub answering {"status": "ok"}). Committing on it could drop a
            # run that never became durable; a retry is always safe because the
            # API dedups on (history_id, automation_id).
            return SubmitResult(
                SubmitOutcome.INVALID_RESPONSE,
                http_status=status_code,
                error="unrecognised acknowledgement",
            )
        return SubmitResult(
            _ACKNOWLEDGED[response.status],
            http_status=status_code,
            response=response,
        )
    if status_code == CAPACITY_STATUS:
        return SubmitResult(
            SubmitOutcome.CAPACITY, http_status=status_code, error="capacity"
        )
    if status_code in DEFINITIVE_REFUSAL_STATUSES and _is_refusal_body(body):
        return SubmitResult(
            SubmitOutcome.REJECTED, http_status=status_code, error=f"http_{status_code}"
        )
    return SubmitResult(
        SubmitOutcome.API_ERROR, http_status=status_code, error=f"http_{status_code}"
    )


def _is_refusal_body(body: bytes) -> bool:
    """Whether a 4xx body is the submit endpoint's own typed refusal.

    A skip commits past the message for good, so the status code alone is not
    enough: a misrouted `AUTOMATION_API_URL` (the gateway, a wrong path prefix,
    an image without the route) answers *every* message with a framework
    default like `404 {"detail": "Not Found"}`, and treating that as a refusal
    would drain the topic. Only the contract's refusal shapes count —
    `{"status": "rejected", ...}` or the validation `{"errors": [{code...}]}`.
    """
    try:
        data = json.loads(body)
    except (TypeError, ValueError):
        return False
    if not isinstance(data, dict):
        return False
    if data.get("status") == REFUSAL_STATUS:
        return True
    errors = data.get("errors")
    return (
        isinstance(errors, list)
        and bool(errors)
        and all(isinstance(error, dict) and "code" in error for error in errors)
    )


def _parse_acknowledgement(body: bytes) -> Optional[SubmitResponse]:
    try:
        data = json.loads(body)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    try:
        status = SubmitStatus(data.get("status"))
    except ValueError:
        return None
    run_id = data.get("run_id")
    reason = data.get("suppression_reason")
    return SubmitResponse(
        status=status,
        run_id=run_id if isinstance(run_id, str) else None,
        suppression_reason=reason if isinstance(reason, str) else None,
    )
