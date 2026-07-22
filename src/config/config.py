"""Minimal env-var config reader (mirrors the `config(...)` call style used in
keep-event-handler). Skeleton — no external config lib pulled in yet.
"""
import os


def config(key: str, default=None, cast=None):
    value = os.environ.get(key, default)
    if cast is not None and value is not None:
        if cast is bool:
            return str(value).lower() in ("1", "true", "yes", "on")
        return cast(value)
    return value
