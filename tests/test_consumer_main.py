"""Application composition-root tests."""

import pytest

from src.bl.pipeline import SubmitPipeline
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

    class FakeSubmitClient:
        def close(self):
            events.append("submit_client_closed")

    class FakeConsumer:
        def __init__(self, *, pipeline, worker_pool):
            assert isinstance(pipeline, SubmitPipeline)
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
    monkeypatch.setattr(consumer_main, "build_submit_client", FakeSubmitClient)
    monkeypatch.setattr(
        consumer_main,
        "reset_redis_client",
        lambda: events.append("redis_closed"),
    )

    consumer_main.main()

    assert "wired" in events
    assert "started" in events
    assert ("shutdown", True) in events
    assert events[-2:] == ["submit_client_closed", "redis_closed"]


@pytest.mark.parametrize("url", ["", "automation-api:8083", "ftp://api", "http://"])
def test_a_missing_or_malformed_api_url_fails_startup(monkeypatch, url):
    import src.consumer_main as consumer_main

    monkeypatch.setattr(consumer_main, "AUTOMATION_API_URL", url)

    with pytest.raises(RuntimeError, match="AUTOMATION_API_URL"):
        consumer_main.build_submit_client()


def test_main_exits_before_touching_kafka_without_an_api_url(monkeypatch):
    import src.consumer_main as consumer_main

    monkeypatch.setattr(consumer_main, "AUTOMATION_API_URL", "")
    monkeypatch.setattr(consumer_main, "start_metrics_server", lambda _: None)
    monkeypatch.setattr(consumer_main, "create_health_server", lambda _: None)

    def no_kafka(**_):
        raise AssertionError("the consumer must not start")

    monkeypatch.setattr(consumer_main, "MatchedAlertConsumer", no_kafka)

    with pytest.raises(SystemExit) as excinfo:
        consumer_main.main()

    assert excinfo.value.code == 1


def test_missing_service_token_is_flagged_but_not_fatal(monkeypatch):
    import src.consumer_main as consumer_main
    from prometheus_client import REGISTRY

    monkeypatch.setattr(consumer_main, "AUTOMATION_API_URL", "http://api.test:8083")
    monkeypatch.setattr(consumer_main, "INTERNAL_SERVICE_TOKEN", "")

    client = consumer_main.build_submit_client()
    client.close()

    assert (
        REGISTRY.get_sample_value(
            "keep_automation_consumer_submit_config_missing",
            {"setting": "service_token"},
        )
        == 1
    )
