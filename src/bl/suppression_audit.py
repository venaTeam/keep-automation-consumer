"""Suppression auditing — stub until the automation API's audit surface exists.

A suppressed message is a message that is **not** submitted, so nothing else in
the system would ever record it. Spec §7.5: "suppressions are visible, never
silent". Until D17/D19 expose the endpoint that writes a `suppressed` audit row
(`automation_runs.state='suppressed'`, `suppression_reason`), this module keeps
the call site real and the implementation swappable — the same Protocol + stub
shape `keep-automation-api` uses for `GitClient`.

Only D17's client replaces `get_suppression_auditor()`; no call site changes.
"""

import logging
from typing import Optional, Protocol

logger = logging.getLogger(__name__)

# `automation_runs.suppression_reason` values (contracts §DB enums). Only
# `duplicate` is reachable from C9; `cooldown` is armed by C10.
REASON_DUPLICATE = "duplicate"
REASON_COOLDOWN = "cooldown"


class SuppressionAuditor(Protocol):
    def record_suppression(
        self,
        *,
        tenant_id: str,
        automation_id: str,
        history_id: Optional[str],
        fingerprint: Optional[str],
        reason: str,
        gate_flags: Optional[dict] = None,
    ) -> None: ...


class LoggingSuppressionAuditor:
    """Logs the audit row that D17 will persist.

    Deliberately lossy and deliberately loud: the log line carries every field
    the row needs, so a duplicate suppressed before D17 lands is still traceable
    to `(tenant, automation, history_id)` — it just is not queryable.
    """

    def record_suppression(
        self,
        *,
        tenant_id: str,
        automation_id: str,
        history_id: Optional[str],
        fingerprint: Optional[str],
        reason: str,
        gate_flags: Optional[dict] = None,
    ) -> None:
        logger.info(
            "automations: suppressed run (audit pending D17) tenant_id=%s "
            "automation_id=%s history_id=%s fingerprint=%s reason=%s gate_flags=%s",
            tenant_id,
            automation_id,
            history_id,
            fingerprint,
            reason,
            gate_flags,
        )


_auditor: Optional[SuppressionAuditor] = None


def get_suppression_auditor() -> SuppressionAuditor:
    global _auditor
    if _auditor is None:
        _auditor = LoggingSuppressionAuditor()
    return _auditor


def set_suppression_auditor(auditor: Optional[SuppressionAuditor]) -> None:
    """Swap the implementation (D17's API client; tests)."""
    global _auditor
    _auditor = auditor
