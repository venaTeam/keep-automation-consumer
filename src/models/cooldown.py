"""Cooldown gate domain values and decisions."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class CooldownOutcome(str, Enum):
    DISABLED = "disabled"
    CLAIMED = "claimed"
    SUPPRESSED = "suppressed"
    MISSING_FIELD = "missing_field"
    FAIL_OPEN = "fail_open"


@dataclass(frozen=True)
class CooldownDecision:
    outcome: CooldownOutcome
    key: Optional[str] = None
    entity_hash: Optional[str] = None
    cooldown_seconds: Optional[int] = None
    owner_token: Optional[str] = field(default=None, repr=False)
    eligible_again_in_seconds: Optional[int] = None
    next_eligible_at: Optional[datetime] = None
    missing_fields: tuple[str, ...] = ()
    gate_flags: dict[str, str] = field(default_factory=dict)

    @property
    def should_submit(self) -> bool:
        return self.outcome is not CooldownOutcome.SUPPRESSED
