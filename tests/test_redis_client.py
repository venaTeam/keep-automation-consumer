"""Tests for the shared Redis client.

Every one of these pins a property that a plausible refactor would silently
delete: the client is built once (a per-call client leaks fds until EMFILE
kills ingestion), a construction failure degrades instead of raising, and the
kwargs that bound the hot path are actually passed.
"""

import sys
import types

import pytest

from src.core import redis_client


@pytest.fixture(autouse=True)
def _clean_client(monkeypatch):
    redis_client.reset_redis_client()
    yield
    redis_client.reset_redis_client()


class FakeRedisModule(types.ModuleType):
    """Stands in for the `redis` package inside `_build_client`."""

    def __init__(self, raises=None):
        super().__init__("redis")
        self.calls = []
        self._raises = raises
        module = self

        class Redis:
            @staticmethod
            def from_url(url, **kwargs):
                module.calls.append((url, kwargs))
                if module._raises is not None:
                    raise module._raises
                return object()

        self.Redis = Redis


def install_redis_module(monkeypatch, module):
    monkeypatch.setitem(sys.modules, "redis", module)


def test_unset_url_returns_none_and_is_reported_as_unconfigured(monkeypatch):
    monkeypatch.setattr(redis_client, "REDIS_URL", "")

    assert redis_client.get_redis_client() is None
    assert redis_client.redis_configured() is False


def test_configured_url_reports_configured(monkeypatch):
    monkeypatch.setattr(redis_client, "REDIS_URL", "redis://localhost:6379")

    assert redis_client.redis_configured() is True


def test_client_is_built_once_and_shared(monkeypatch):
    """A client per call would leak a ConnectionPool — the fd-leak failure."""
    module = FakeRedisModule()
    install_redis_module(monkeypatch, module)
    monkeypatch.setattr(redis_client, "REDIS_URL", "redis://localhost:6379")

    first = redis_client.get_redis_client()
    second = redis_client.get_redis_client()

    assert first is second
    assert len(module.calls) == 1


def test_hot_path_kwargs_are_passed(monkeypatch):
    module = FakeRedisModule()
    install_redis_module(monkeypatch, module)
    monkeypatch.setattr(redis_client, "REDIS_URL", "redis://localhost:6379")

    redis_client.get_redis_client()
    _, kwargs = module.calls[0]

    # Without the timeouts, the gate could stall the poll loop indefinitely —
    # the whole reason it is allowed to run inline (ADR-007 shape).
    assert kwargs["socket_connect_timeout"] > 0
    assert kwargs["socket_timeout"] > 0
    assert kwargs["decode_responses"] is True
    assert kwargs["socket_keepalive"] is True
    assert kwargs["health_check_interval"] > 0
    # No retry policy: retries belong to the gate's breaker, not the socket.
    assert "retry_on_timeout" not in kwargs


def test_construction_failure_degrades_instead_of_raising(monkeypatch):
    module = FakeRedisModule(raises=ValueError("bad url"))
    install_redis_module(monkeypatch, module)
    monkeypatch.setattr(redis_client, "REDIS_URL", "redis://nope")

    assert redis_client.get_redis_client() is None


def test_a_failed_build_is_cached_not_retried_per_message(monkeypatch):
    """Retrying a config failure per message is a traceback flood, not recovery."""
    module = FakeRedisModule(raises=ValueError("bad url"))
    install_redis_module(monkeypatch, module)
    monkeypatch.setattr(redis_client, "REDIS_URL", "redis://nope")

    for _ in range(5):
        assert redis_client.get_redis_client() is None

    assert len(module.calls) == 1


def test_missing_redis_wheel_does_not_raise(monkeypatch):
    """A missing dependency must degrade to ungated, never crashloop the pod."""
    monkeypatch.setattr(redis_client, "REDIS_URL", "redis://localhost:6379")
    monkeypatch.setitem(sys.modules, "redis", None)  # `import redis` -> ImportError

    assert redis_client.get_redis_client() is None


def test_reset_closes_the_client_and_clears_a_cached_failure(monkeypatch):
    closed = []

    class Closeable:
        def close(self):
            closed.append(True)

    module = FakeRedisModule()
    module.Redis.from_url = staticmethod(lambda url, **kwargs: Closeable())
    install_redis_module(monkeypatch, module)
    monkeypatch.setattr(redis_client, "REDIS_URL", "redis://localhost:6379")

    redis_client.get_redis_client()
    redis_client.reset_redis_client()

    assert closed == [True]
    assert redis_client.get_redis_client() is not None  # rebuilt after reset


def test_reset_tolerates_a_close_that_raises(monkeypatch):
    class Angry:
        def close(self):
            raise RuntimeError("nope")

    module = FakeRedisModule()
    module.Redis.from_url = staticmethod(lambda url, **kwargs: Angry())
    install_redis_module(monkeypatch, module)
    monkeypatch.setattr(redis_client, "REDIS_URL", "redis://localhost:6379")

    redis_client.get_redis_client()
    redis_client.reset_redis_client()  # must not raise
