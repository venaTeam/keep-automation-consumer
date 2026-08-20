"""Minimal env-var config reader (mirrors the `config(...)` call style used in
keep-event-handler). Skeleton — no external config lib pulled in yet.
"""
import os

from dotenv import find_dotenv, load_dotenv


# Configuration constants are evaluated at import time. Load local development
# values before any caller can import `src.config.consts` and freeze defaults.
load_dotenv(find_dotenv(usecwd=True))


def config(key: str, default=None, cast=None):
    value = os.environ.get(key, default)
    if cast is not None and value is not None:
        if cast is bool:
            return str(value).lower() in ("1", "true", "yes", "on")
        return cast(value)
    return value
