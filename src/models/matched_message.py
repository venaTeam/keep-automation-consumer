"""Matched-alert message shape (contracts §"Matched message").

The B5 producer uses `alert.id` for event identity. The existing consumer/API
name `history_id` maps to that value; older queued payloads may use `history_id`.
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
    alert: dict            # full alert payload snapshot (carries id, fingerprint, time_created)
    automation_id: str     # match result + Kafka partition key
    matched_m: Optional[int] = None   # observed fan-out M (M <= 3 guardrail)
    cooldown: Optional[dict] = None    # resolved cooldown config, or None when off

    @property
    def history_id(self) -> Optional[str]:
        """Map B5's wire ID to the existing gate/audit name without editing alert.

        Fall back only when `id` is absent, for legacy queued messages. When
        both fields exist, the producer's canonical `id` always takes priority.
        """
        return self.alert.get("id", self.alert.get("history_id"))

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
