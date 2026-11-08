"""Suppression auditing — stub until the automation API's audit surface exists.

A suppressed message is a message that is **not** submitted, so nothing else in
the system would ever record it. Spec §7.5: "suppressions are visible, never
silent". Until D17/D19 expose the endpoint that writes a `suppressed` audit row
(`automation_runs.state='suppressed'`, `suppression_reason`), this module keeps
the call site real and the implementation swappable — the same Protocol + stub
shape `keep-automation-api` uses for `GitClient`.

Only composition-root wiring changes when D17's client replaces the stub.
"""

import logging
from typing import Any, Optional, Protocol

from src.core.safe_logging import safe_log_value

logger = logging.getLogger(__name__)

# `automation_runs.suppression_reason` values (contracts §DB enums): `duplicate`
# from the idempotency gate, `cooldown` from the cooldown gate.
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
        gate_flags: Optional[dict[str, str]] = None,
        detail: Optional[dict[str, Any]] = None,
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
        gate_flags: Optional[dict[str, str]] = None,
        detail: Optional[dict[str, Any]] = None,
    ) -> None:
        # `detail` carries reason-specific context the row will need — for a
        # cooldown suppression, the entity hash and "eligible again in at least
        # T" (spec §5.2 step 2).
        logger.info(
            "automations: suppressed run (audit pending D17) tenant_id=%s "
            "automation_id=%s history_id=%s fingerprint=%s reason=%s gate_flags=%s "
            "detail=%s",
            safe_log_value(tenant_id),
            safe_log_value(automation_id),
            safe_log_value(history_id),
            safe_log_value(fingerprint),
            safe_log_value(reason),
            safe_log_value(gate_flags),
            safe_log_value(detail),
        )
