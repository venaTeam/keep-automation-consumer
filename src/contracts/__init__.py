"""Consumer-side copies of contracts pinned by automation-contracts.md."""

from src.contracts.cooldown import (
    COOLDOWN_ARMED_VALUE,
    COOLDOWN_PROVISIONAL_TTL_SECONDS,
    COOLDOWN_SCHEME_VER,
    MissingCooldownField,
    canonical_cooldown_bytes,
    canonical_cooldown_hash,
    cooldown_key,
)

__all__ = [
    "COOLDOWN_ARMED_VALUE",
    "COOLDOWN_PROVISIONAL_TTL_SECONDS",
    "COOLDOWN_SCHEME_VER",
    "MissingCooldownField",
    "canonical_cooldown_bytes",
    "canonical_cooldown_hash",
    "cooldown_key",
]
