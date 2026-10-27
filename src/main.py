"""FastAPI health/metrics app for keep-automation-consumer (K8s probes + scrape).

The Kafka consume loop runs standalone in `consumer_main.py`; this app only
exposes health + metrics.
"""
import logging

from fastapi import FastAPI

from src import logging_conf
from src.api.routes.v1 import health, metrics
from src.core import metrics as _consumer_metrics  # noqa: F401  (register counters for scrape)

logging_conf.setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Keep Automation Consumer",
    description="Health/metrics for the matched-alerts consumer service",
)

app.include_router(health.router, prefix="/v1", tags=["health"])
app.include_router(health.router, tags=["root"])
app.include_router(metrics.router, prefix="/v1", tags=["metrics"])
app.include_router(metrics.router, tags=["metrics"])
