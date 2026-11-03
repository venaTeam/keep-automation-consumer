"""Matched-alert message shape (contracts §"Matched message").

`automation-contracts.md` is authoritative; this model follows it, never the reverse.
Deliberately a dependency-free dataclass: there is no shared contracts package —
each repo mirrors only the slice of the doc it uses, kept honest by tests.

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
        message = cls(
            tenant_id=data["tenant_id"],
            alert=data["alert"],
            automation_id=data["automation_id"],
            matched_m=data.get("matched_m"),
            cooldown=data.get("cooldown"),
        )
        message.validate()
        return message

    def validate(self) -> None:
        """Reject a message no submit could ever carry (contracts §Matched message).

        A null tenant/automation or a missing `history_id` is refused by the
        API's request validation on every attempt, and NULL would not dedup in
        the run table's unique key either. Retrying it would stall its
        partition forever, so it is poison: rejected here, skipped by the
        consumer.
        """
        problems = []
        if not _non_empty_str(self.tenant_id):
            problems.append("tenant_id")
        if not _non_empty_str(self.automation_id):
            problems.append("automation_id")
        if not isinstance(self.alert, dict):
            problems.append("alert")
        elif not _non_empty_str(self.history_id):
            problems.append("alert.history_id")
        if problems:
            raise MalformedMessage(problems)


class MalformedMessage(ValueError):
    def __init__(self, fields: list[str]) -> None:
        super().__init__(f"invalid or missing: {', '.join(fields)}")
        self.fields = fields


def _non_empty_str(value: object) -> bool:
    return isinstance(value, str) and bool(value)
