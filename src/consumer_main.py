#!/usr/bin/env python3
"""Standalone matched-alerts consumer entrypoint (C8 skeleton).

Starts a Prometheus metrics server + a stdlib health server, then runs the
blocking Kafka consume loop. Mirrors keep-event-handler's consumer_main.py.
"""
import logging
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import find_dotenv, load_dotenv
from prometheus_client import start_http_server

from src import logging_conf
from src.config.consts import HEALTH_CHECK_PORT, PROMETHEUS_METRICS_PORT

load_dotenv(find_dotenv())
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


def main() -> None:
    logger.info("Starting Keep Automation Consumer (matched-alerts)")
    try:
        start_metrics_server(PROMETHEUS_METRICS_PORT)
        create_health_server(HEALTH_CHECK_PORT)

        from src.core.kafka_consumer import MatchedAlertConsumer

        MatchedAlertConsumer().start()  # blocks until shutdown
    except KeyboardInterrupt:
        logger.info("Interrupted, shutting down")
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        sys.exit(1)
    logger.info("Consumer shutdown complete")


if __name__ == "__main__":
    main()
