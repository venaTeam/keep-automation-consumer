"""Application composition-root tests."""

import pytest

from src.bl.suppression_audit import LoggingSuppressionAuditor
from src.core.kafka_consumer import MatchedAlertConsumer


def test_consumer_dependencies_are_required():
    with pytest.raises(TypeError):
        MatchedAlertConsumer()


def test_main_wires_dependencies_and_releases_process_resources(monkeypatch):
    import src.consumer_main as consumer_main

    events = []

    class FakePool:
        def __init__(self, max_workers):
            events.append(("pool", max_workers))

        def shutdown(self, *, wait):
            events.append(("shutdown", wait))

    class FakeConsumer:
        def __init__(self, *, idempotency_gate, suppression_auditor, worker_pool):
            assert idempotency_gate is not None
            assert isinstance(suppression_auditor, LoggingSuppressionAuditor)
            assert isinstance(worker_pool, FakePool)
            self._worker_pool = worker_pool
            events.append("wired")

        def start(self):
            events.append("started")
            self._worker_pool.shutdown(wait=True)

    monkeypatch.setattr(consumer_main, "BoundedWorkerPool", FakePool)
    monkeypatch.setattr(consumer_main, "MatchedAlertConsumer", FakeConsumer)
    monkeypatch.setattr(consumer_main, "start_metrics_server", lambda _: None)
    monkeypatch.setattr(consumer_main, "create_health_server", lambda _: None)
    monkeypatch.setattr(consumer_main, "configure_redis", lambda: None)
    monkeypatch.setattr(
        consumer_main,
        "reset_redis_client",
        lambda: events.append("redis_closed"),
    )

    consumer_main.main()

    assert "wired" in events
    assert "started" in events
    assert ("shutdown", True) in events
    assert events[-1] == "redis_closed"
