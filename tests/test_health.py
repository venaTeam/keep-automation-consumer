"""C8 skeleton tests: health/metrics app boots and responds."""
from fastapi.testclient import TestClient

from src.main import app

client = TestClient(app)


def test_health_ok():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_root_ok():
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["service"] == "keep-automation-consumer"


def test_metrics_endpoint():
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "keep_automation_consumer_messages_consumed_total" in resp.text
