"""Cooldown key contract copied from ``automation-contracts.md``.

The markdown contract is authoritative.  This module is deliberately small so
its byte-level canonicalization is easy to compare with every future producer
or consumer:

    sort fields -> JSON ``[field, value]`` pairs -> UTF-8 -> SHA-256
"""

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

COOLDOWN_SCHEME_VER = 1
COOLDOWN_PROVISIONAL_TTL_SECONDS = 30


class MissingCooldownField(ValueError):
    """A declared entity field is absent; the gate must run without a key."""

    def __init__(self, fields: Sequence[str]) -> None:
        self.fields = tuple(fields)
        super().__init__(f"missing cooldown field(s): {', '.join(self.fields)}")


def canonical_cooldown_hash(
    alert: Mapping[str, Any], fields: Sequence[str]
) -> str:
    """Return the scheme-1 entity digest exactly as the contract specifies."""
    return hashlib.sha256(canonical_cooldown_bytes(alert, fields)).hexdigest()


def canonical_cooldown_bytes(
    alert: Mapping[str, Any], fields: Sequence[str]
) -> bytes:
    """Return the exact UTF-8 bytes hashed by scheme 1."""
    if any(not isinstance(field, str) for field in fields):
        raise TypeError("cooldown fields must be strings")
    if "history_id" in fields:
        raise ValueError("history_id must never participate in a cooldown key")

    sorted_fields = sorted(fields)
    missing = [field for field in sorted_fields if field not in alert]
    if missing:
        raise MissingCooldownField(missing)

    pairs = [[field, alert[field]] for field in sorted_fields]
    return json.dumps(
        pairs,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def cooldown_key(automation_id: str, scheme_ver: int, entity_hash: str) -> str:
    """``cooldown:{automation_id}:{scheme_ver}:{hash}``, verbatim."""
    return f"cooldown:{automation_id}:{scheme_ver}:{entity_hash}"
