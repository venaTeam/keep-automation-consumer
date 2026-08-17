"""Process-lifetime Redis client for the consumer gates.

**One client per process.** `Redis.from_url()` builds a NEW `ConnectionPool`
every call, so constructing a client per message (or per reconnect) leaks file
descriptors into the Kafka consumer process until EMFILE stops librdkafka
opening sockets and ingestion dies. That is this platform's signature failure —
keep-event-handler's `src/bl/automations/pubsub.py` carries the same warning and
the same build-once shape. Reuse the client; the pool underneath reconnects.

**Absent Redis is a supported state.** `REDIS_URL` unset returns `None`, and the
gates treat `None` exactly like an unreachable Redis: fail open (spec §4.5,
§6.3). Nothing here raises on the hot path.
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
_lock = threading.Lock()


def get_redis_client():
    """The shared client, or `None` when no `REDIS_URL` is configured.

    Construction failure (bad URL, missing driver) is reported once and then
    behaves like an unset URL — a gate that cannot be armed must never take the
    consumer down with it.
    """
    global _client
    if not REDIS_URL:
        return None
    if _client is not None:
        return _client
    with _lock:
        if _client is None:
            _client = _build_client()
    return _client


def _build_client():
    import redis

    try:
        return redis.Redis.from_url(
            REDIS_URL,
            # Gate values are short ASCII tokens ("pending"/"done") compared as
            # strings; decoding here keeps that comparison out of every caller.
            decode_responses=True,
            socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
            socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
            socket_keepalive=True,
            health_check_interval=_HEALTH_CHECK_INTERVAL_SECONDS,
        )
    except Exception:  # noqa: BLE001 - construction must not kill the consumer
        logger.exception(
            "automations: could not build the Redis client; gates fail open"
        )
        return None


def reset_redis_client() -> None:
    """Drop the cached client (tests, and re-reading config after a reload)."""
    global _client
    with _lock:
        client = _client
        _client = None
    if client is not None:
        try:
            client.close()
        except Exception:  # noqa: BLE001 - closing must never raise onward
            logger.debug("automations: error closing the redis client", exc_info=True)
