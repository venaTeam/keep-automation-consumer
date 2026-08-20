"""Cooldown gate domain values."""

from enum import Enum


class CooldownOutcome(str, Enum):
    DISABLED = "disabled"
    CLAIMED = "claimed"
    SUPPRESSED = "suppressed"
    MISSING_FIELD = "missing_field"
    FAIL_OPEN = "fail_open"
