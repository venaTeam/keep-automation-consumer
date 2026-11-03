"""Atomic Redis lifecycle operations for owned cooldown claims."""

import threading
from typing import Any, Protocol

from src.bl.gates.cooldown_key import COOLDOWN_ARMED_VALUE

_ACTIVATE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
  return 1
end
return 0
""".strip()

_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
""".strip()

# `reclaim` results. RECLAIM_REENTERED is evidence the run is recent: a
# delivery of this event held the claim within the provisional TTL.
RECLAIM_LOST = 0
RECLAIM_CLAIMED = 1
RECLAIM_REENTERED = 2

# Runs only after a plain `SET NX` lost. The run_id is derived per event, so
# our own token still there means a delivery of this same event claimed it — an
# earlier attempt whose release (or claim reply) never landed, a delivery killed
# mid-submit, or a sibling still in flight: re-enter and refresh. A key that
# vanished in between is simply claimed. Anything else is another event's.
_RECLAIM_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
  return 2
end
if not current then
  if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) then
    return 1
  end
end
return 0
""".strip()


class RedisScript(Protocol):
    def __call__(
        self,
        *,
        keys: list[str],
        args: list[Any],
        client: Any = None,
    ) -> Any: ...


class RedisCooldownClient(Protocol):
    def set(
        self,
        key: str,
        value: str,
        *,
        nx: bool = False,
        ex: int | None = None,
    ) -> Any: ...

    def ttl(self, key: str) -> int: ...

    def register_script(self, script: str) -> RedisScript: ...


class CooldownRedisLifecycle:
    """Register and reuse atomic activation/release scripts per Redis client."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._client: RedisCooldownClient | None = None
        self._activate_script: RedisScript | None = None
        self._release_script: RedisScript | None = None
        self._reclaim_script: RedisScript | None = None

    def reclaim(
        self,
        client: RedisCooldownClient,
        *,
        key: str,
        owner_token: str,
        ttl_seconds: int,
    ) -> int:
        """Re-enter a claim this token already holds (or claim a vanished key).

        Returns RECLAIM_REENTERED, RECLAIM_CLAIMED or RECLAIM_LOST.
        """
        _, _, reclaim = self._scripts(client)
        return int(reclaim(keys=[key], args=[owner_token, ttl_seconds], client=client))

    def activate(
        self,
        client: RedisCooldownClient,
        *,
        key: str,
        owner_token: str,
        cooldown_seconds: int,
    ) -> bool:
        activate, _, _ = self._scripts(client)
        return bool(
            activate(
                keys=[key],
                args=[owner_token, COOLDOWN_ARMED_VALUE, cooldown_seconds],
                client=client,
            )
        )

    def release(
        self,
        client: RedisCooldownClient,
        *,
        key: str,
        owner_token: str,
    ) -> bool:
        _, release, _ = self._scripts(client)
        return bool(
            release(
                keys=[key],
                args=[owner_token],
                client=client,
            )
        )

    def _scripts(
        self, client: RedisCooldownClient
    ) -> tuple[RedisScript, RedisScript, RedisScript]:
        with self._lock:
            if client is not self._client:
                self._activate_script = client.register_script(_ACTIVATE_SCRIPT)
                self._release_script = client.register_script(_RELEASE_SCRIPT)
                self._reclaim_script = client.register_script(_RECLAIM_SCRIPT)
                self._client = client
            assert self._activate_script is not None
            assert self._release_script is not None
            assert self._reclaim_script is not None
            return self._activate_script, self._release_script, self._reclaim_script
