"""Matched-alert message shape (contracts §"Matched message").

`automation-contracts.md` is authoritative; this model follows it, never the reverse.
Skeleton uses a dependency-free dataclass. The A2 contracts package (Pydantic v1)
supersedes this field-for-field once vendored; kept minimal here on purpose.

Required top-level keys (`tenant_id`, `alert`, `automation_id`) are read with `[]`,
so a message missing one raises `KeyError` instead of deserializing into a partial
or untenanted message. Defaulting a tenant would be a cross-tenant bug.
"""
import json
from dataclasses import dataclass
from typing import Optional


@dataclass
class MatchedAlertMessage:
    tenant_id: str         # owning tenant (a `tenant.id` value); stamped by the matcher, not in `alert`
    alert: dict            # full alert payload snapshot (carries history_id, fingerprint, time_created)
    automation_id: str     # match result + Kafka partition key
    matched_m: Optional[int] = None   # observed fan-out M (M <= 3 guardrail)
    cooldown: Optional[dict] = None    # resolved cooldown config, or None when off

    @property
    def history_id(self) -> Optional[str]:
        return self.alert.get("history_id")

    @property
    def fingerprint(self) -> Optional[str]:
        return self.alert.get("fingerprint")

    @classmethod
    def from_bytes(cls, raw: bytes) -> "MatchedAlertMessage":
        data = json.loads(raw)
        return cls(
            tenant_id=data["tenant_id"],
            alert=data["alert"],
            automation_id=data["automation_id"],
            matched_m=data.get("matched_m"),
            cooldown=data.get("cooldown"),
        )
