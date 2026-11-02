"""Submit request/response shapes (contracts §"Submit").

`automation-contracts.md` is authoritative; these follow it, never the reverse.
Plain dataclasses for the same reason as `matched_message.py`: there is no
shared contracts package, only this repo's mirror of the slice it uses.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class SubmitStatus(str, Enum):
    """`SubmitResponse.status` — an acknowledgement, never a run outcome."""

    ACCEPTED = "accepted"
    ALREADY_SUBMITTED = "already_submitted"
    SUPPRESSED = "suppressed"


class SubmitOutcome(str, Enum):
    """How the consumer classifies one submit attempt (contracts §Submit,
    "Consumer response classification"). Also the metric label set."""

    ACCEPTED = "accepted"
    ALREADY_SUBMITTED = "already_submitted"
    SUPPRESSED = "suppressed"
    # 429 {status: capacity}: the API's in-flight ceiling; no row exists.
    CAPACITY = "capacity"
    # 400/404/409/422: the API refused this message for good.
    REJECTED = "rejected"
    # 5xx, 408, 401, 403, any other non-200: a clean API failure.
    API_ERROR = "api_error"
    # Connect error, timeout, protocol error: no HTTP response at all.
    TRANSPORT_ERROR = "transport_error"
    # HTTP 200 without a known `status`: not an acknowledgement.
    INVALID_RESPONSE = "invalid_response"


ACKNOWLEDGED_OUTCOMES = frozenset(
    {
        SubmitOutcome.ACCEPTED,
        SubmitOutcome.ALREADY_SUBMITTED,
        SubmitOutcome.SUPPRESSED,
    }
)


@dataclass(frozen=True)
class SubmitRequest:
    tenant_id: str
    automation_id: str
    history_id: Optional[str]
    fingerprint: Optional[str]
    run_id: str
    alert: dict
    gate_flags: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "automation_id": self.automation_id,
            "history_id": self.history_id,
            "fingerprint": self.fingerprint,
            "run_id": self.run_id,
            "alert": self.alert,
            "gate_flags": self.gate_flags,
        }


@dataclass(frozen=True)
class SubmitResponse:
    status: SubmitStatus
    run_id: Optional[str] = None
    suppression_reason: Optional[str] = None


@dataclass(frozen=True)
class SubmitResult:
    outcome: SubmitOutcome
    http_status: Optional[int] = None
    response: Optional[SubmitResponse] = None
    # Short, credential-free description for logs (an exception class name or
    # an HTTP status) — never a response body, which the caller does not own.
    error: Optional[str] = None

    @property
    def acknowledged(self) -> bool:
        """Any HTTP 200 acknowledgement: the run row is durable, commit."""
        return self.outcome in ACKNOWLEDGED_OUTCOMES
