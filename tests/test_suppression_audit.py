"""Tests for the suppression-audit stub.

The point of these is the call-site contract: the consumer calls
`record_suppression` with a fixed keyword set, and D17 will swap in an HTTP
client that must accept exactly that set. A test fake with `**kwargs` cannot
catch a signature drift — the real implementation has to be called.
"""

import inspect
import logging

from src.bl.suppression_audit import (
    REASON_COOLDOWN,
    REASON_DUPLICATE,
    LoggingSuppressionAuditor,
)

# The exact keyword set `MatchedAlertConsumer._handle` passes.
CALL_SITE_KWARGS = {
    "tenant_id": "keep",
    "automation_id": "3f2b8c1e-4d6a-4b2f-9e77-0a1b2c3d4e5f",
    "history_id": "evt-789",
    "fingerprint": "abc123",
    "reason": REASON_DUPLICATE,
    "gate_flags": {"idempotency": "duplicate"},
}


def test_the_real_auditor_accepts_the_call_sites_keyword_set():
    """A renamed parameter here is a TypeError on the first duplicate."""
    LoggingSuppressionAuditor().record_suppression(**CALL_SITE_KWARGS)


def test_the_consumer_call_site_matches_the_protocol_signature():
    from src.bl.suppression_audit import SuppressionAuditor

    parameters = inspect.signature(SuppressionAuditor.record_suppression).parameters

    assert set(CALL_SITE_KWARGS) <= set(parameters)


def test_gate_flags_is_optional():
    kwargs = {k: v for k, v in CALL_SITE_KWARGS.items() if k != "gate_flags"}

    LoggingSuppressionAuditor().record_suppression(**kwargs)


def test_the_audit_line_carries_every_field_the_row_needs(caplog):
    with caplog.at_level(logging.INFO, logger="src.bl.suppression_audit"):
        LoggingSuppressionAuditor().record_suppression(**CALL_SITE_KWARGS)

    record = caplog.records[-1].getMessage()
    for value in ("keep", "evt-789", "abc123", REASON_DUPLICATE):
        assert value in record


def test_reason_tokens_match_the_db_enum():
    """contracts §DB enums — automation_runs.suppression_reason."""
    assert REASON_DUPLICATE == "duplicate"
    assert REASON_COOLDOWN == "cooldown"


def test_logging_auditor_has_no_hidden_global_instance():
    first = LoggingSuppressionAuditor()
    second = LoggingSuppressionAuditor()

    assert first is not second
