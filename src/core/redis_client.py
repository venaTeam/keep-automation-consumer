"""Process-lifetime Redis client for the consumer gates.

**One client per process.** `Redis.from_url()` builds a NEW `ConnectionPool`
every call, so constructing a client per message (or per reconnect) leaks file
descriptors into the Kafka consumer process until EMFILE stops librdkafka
opening sockets and ingestion dies. That is this platform's signature failure —
keep-event-handler's `src/bl/automations/pubsub.py` carries the same warning and
the same build-once shape. Reuse the client; the pool underneath reconnects.

**Absent Redis is a supported state.** `REDIS_URL` unset returns `None`, and the
gates treat `None` exactly like an unreachable Redis: fail open (spec §4.5,
§6.3). Nothing here raises on the hot path — including a missing `redis` wheel,
which degrades to ungated submits rather than a crashlooping consumer.

**A failed build is cached.** Construction failures are configuration failures
(bad URL, missing driver), not transient ones. Retrying per message would
re-lock, re-import and re-log a traceback for every message at the ~200 msg/s
target. `reset_redis_client()` clears the failure for a re-read of config.
"""

import logging
import threading

from src.config.consts import (
    REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
    REDIS_SOCKET_TIMEOUT_SECONDS,
    REDIS_URL,
)

logger = logging.getLogger(__name__)

# Redis-py does not PING an idle connection unless this is set. Idle-timeout
# devices between here and Redis (NLB 350s, ELB 60s, most corporate NATs) drop
# the connection silently otherwise.
_HEALTH_CHECK_INTERVAL_SECONDS = 30

_client = None
_build_failed = False
_lock = threading.Lock()


def redis_configured() -> bool:
    """Whether a Redis URL is configured at all.

    Distinct from "reachable": an unset URL is a deployment mistake that runs
    100% ungated forever, while an unreachable one is an incident. The two get
    different signals (`gates_config_missing` vs `redis_errors`), because an
    alert on the second can never fire for the first.
    """
    return bool(REDIS_URL)


def get_redis_client():
    """The shared client, or `None` when Redis is unconfigured or unbuildable.

    `None` is not an error path for callers — the gates fail open on it, which
    is the same behaviour they apply to an unreachable Redis.
    """
    global _client, _build_failed
    if not REDIS_URL or _build_failed:
        return None
    if _client is not None:
        return _client
    with _lock:
        if _client is None and not _build_failed:
            client = _build_client()
            if client is None:
                # Cached: a construction failure will not fix itself, and
                # retrying it per message is a traceback flood inside the poll
                # loop, not a recovery strategy.
                _build_failed = True
            _client = client
    return _client


def _build_client():
    try:
        # Imported here, inside the try, so a missing `redis` wheel degrades to
        # "gates fail open" instead of taking the consumer process down. The
        # import is deliberately not at module scope for the same reason.
        import redis

        return redis.Redis.from_url(
            REDIS_URL,
            # Gate values are short ASCII tokens ("pending"/"done") compared as
            # strings; decoding here keeps that comparison out of every caller.
            decode_responses=True,
            socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
            socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
            socket_keepalive=True,
            health_check_interval=_HEALTH_CHECK_INTERVAL_SECONDS,
            # No retry policy on purpose. redis-py 5.x defaults to one attempt
            # (`Retry(NoBackoff(), 0)`); adding `retry_on_timeout` here would
            # multiply the per-message stall by the attempt count on the very
            # path ADR-007 says must stay bounded. Retries belong to the
            # breaker in the gate, not to the socket.
        )
    except Exception:  # noqa: BLE001 - construction must not kill the consumer
        logger.exception(
            "automations: could not build the Redis client; gates fail open "
            "for the life of this process (fix config and restart)"
        )
        return None


def reset_redis_client() -> None:
    """Drop the cached client and any cached failure (tests, config re-read)."""
    global _client, _build_failed
    with _lock:
        client = _client
        _client = None
        _build_failed = False
    if client is not None:
        try:
            client.close()
        except Exception:  # noqa: BLE001 - closing must never raise onward
            logger.debug("automations: error closing the redis client", exc_info=True)
