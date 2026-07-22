"""C8 skeleton tests: consumer config + matched-message parsing."""
import json

from src.core.kafka_consumer import build_consumer_config
from src.models.matched_message import MatchedAlertMessage


def test_auto_commit_is_off():
    cfg = build_consumer_config()
    assert cfg["enable.auto.commit"] is False


def test_consumer_config_has_group_and_servers():
    cfg = build_consumer_config()
    assert cfg["group.id"]
    assert cfg["bootstrap.servers"]


def test_matched_message_from_bytes():
    raw = json.dumps(
        {
            "alert": {
                "application": "payments",
                "severity": "critical",
                "fingerprint": "abc123",
                "history_id": "evt-789",
                "time_created": "2026-07-12T14:03:00Z",
            },
            "automation_id": "3f2b-uuid",
            "matched_m": 2,
            "cooldown": {"fields": ["site"], "seconds": 300, "scheme_ver": 1},
        }
    ).encode("utf-8")

    msg = MatchedAlertMessage.from_bytes(raw)
    assert msg.automation_id == "3f2b-uuid"
    assert msg.matched_m == 2
    assert msg.history_id == "evt-789"
    assert msg.fingerprint == "abc123"
    assert msg.cooldown["seconds"] == 300
