#!/usr/bin/env python3
"""Standalone matched-alerts consumer entrypoint (C8 skeleton).

Starts a Prometheus metrics server + a stdlib health server, then runs the
blocking Kafka consume loop. Mirrors keep-event-handler's consumer_main.py.
"""
import logging
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from prometheus_client import start_http_server
from redis import Redis

from src import logging_conf
from src.bl.gates.idempotency import IdempotencyGate
from src.bl.suppression_audit import LoggingSuppressionAuditor
from src.config.consts import (
    HEALTH_CHECK_PORT,
    PROMETHEUS_METRICS_PORT,
    WORKER_POOL_SIZE,
)
from src.core.kafka_consumer import MatchedAlertConsumer
from src.core.metrics import (
    GateConfigSetting,
    gate_config_missing_gauges,
)
from src.core.redis_client import (
    get_redis_client,
    redis_configured,
    reset_redis_client,
)
from src.core.worker_pool import BoundedWorkerPool

logging_conf.setup_logging()
logger = logging.getLogger(__name__)


def start_metrics_server(port: int) -> None:
    logger.info("Starting Prometheus metrics server on port %s", port)
    start_http_server(port)


def create_health_server(port: int) -> HTTPServer:
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/health", "/healthz", "/ready", "/"):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "ok"}')
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *_):  # suppress access logs
            pass

    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info("Health check server started on port %s", port)
    return server


def configure_redis() -> Optional[Redis]:
    """Build and report process Redis state before message consumption."""
    redis_url_gauge = gate_config_missing_gauges[GateConfigSetting.REDIS_URL]
    redis_client_gauge = gate_config_missing_gauges[GateConfigSetting.REDIS_CLIENT]

    if not redis_configured():
        redis_url_gauge.set(1)
        redis_client_gauge.set(1)
        logger.warning(
            "automations: REDIS_URL is not set — the idempotency gate is "
            "disabled and every message will be submitted ungated (fail-open). "
            "Duplicate protection falls entirely to the API's unique "
            "(history_id, automation_id) constraint."
        )
        return None

    redis_url_gauge.set(0)
    client = get_redis_client()
    if client is None:
        redis_client_gauge.set(1)
        logger.warning(
            "automations: REDIS_URL is set but the Redis client could not be "
            "built — the idempotency gate is disabled for the life of this "
            "process (fail-open). Fix the configuration and restart."
        )
        return None

    redis_client_gauge.set(0)
    return client


def main() -> None:
    logger.info("Starting Keep Automation Consumer (matched-alerts)")
    try:
        start_metrics_server(PROMETHEUS_METRICS_PORT)
        create_health_server(HEALTH_CHECK_PORT)
        redis_client = configure_redis()
        worker_pool = BoundedWorkerPool(WORKER_POOL_SIZE)
        consumer = MatchedAlertConsumer(
            idempotency_gate=IdempotencyGate(
                client_factory=lambda: redis_client,
            ),
            suppression_auditor=LoggingSuppressionAuditor(),
            worker_pool=worker_pool,
        )
        consumer.start()  # blocks until shutdown
    except KeyboardInterrupt:
        logger.info("Interrupted, shutting down")
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        sys.exit(1)
    finally:
        reset_redis_client()
    logger.info("Consumer shutdown complete")


if __name__ == "__main__":
    main()
