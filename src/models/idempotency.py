"""Shared idempotency domain values."""

from enum import Enum


class IdempotencyOutcome(str, Enum):
    CLAIMED = "claimed"
    DUPLICATE = "duplicate"
    AMBIGUOUS = "ambiguous"
    FAIL_OPEN = "fail_open"
