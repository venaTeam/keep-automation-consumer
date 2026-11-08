"""Structured logging setup (skeleton)."""
import logging
import os


def setup_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # httpx logs every request at INFO — one line per submit, ~200/s at the
    # target rate. Submit outcomes are already metrics and budgeted warnings.
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
