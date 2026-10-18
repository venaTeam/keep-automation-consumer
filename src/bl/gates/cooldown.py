"""Best-effort owned cooldown decisions; C11 wires them into submit flow."""

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional
from uuid import UUID

from src.bl.gates.cooldown_key import (
    COOLDOWN_PROVISIONAL_TTL_SECONDS,
    COOLDOWN_SCHEME_VER,
    MissingCooldownField,
    canonical_cooldown_hash,
    cooldown_key,
)
from src.bl.gates.cooldown_redis import (
    CooldownRedisLifecycle,
    RedisCooldownClient,
)
from src.config.consts import REDIS_ERROR_LOG_INTERVAL_SECONDS
from src.core.metrics import (
    RedisOperation,
    cooldown_decision_counters,
    redis_error_counters,
)
from src.models.cooldown import CooldownDecision, CooldownOutcome
from src.models.matched_message import MatchedAlertMessage

logger = logging.getLogger(__name__)

REASON_REDIS_UNAVAILABLE = "redis_unavailable"
REASON_INVALID_CONFIG = "invalid_config"
REASON_MISSING_FIELD = "missing_field"

RedisClientFactory = Callable[[], Optional[RedisCooldownClient]]
WallClock = Callable[[], float]
MonotonicClock = Callable[[], float]


class CooldownGate:
    def __init__(
        self,
        client_factory: RedisClientFactory,
        wall_clock: WallClock = time.time,
        monotonic_clock: MonotonicClock = time.monotonic,
        error_log_interval_seconds: float = REDIS_ERROR_LOG_INTERVAL_SECONDS,
    ) -> None:
        self._client_factory = client_factory
        self._wall_clock = wall_clock
        self._monotonic_clock = monotonic_clock
        self._error_log_interval_seconds = max(0.0, error_log_interval_seconds)
        self._next_error_log_at = 0.0
        self._log_lock = threading.Lock()
        self._lifecycle = CooldownRedisLifecycle()

    def claim(
        self, message: MatchedAlertMessage, *, run_id: str
    ) -> CooldownDecision:
        config = message.cooldown
        if config is None:
            return self._decide(CooldownOutcome.DISABLED)

        parsed = self._parse_config(config)
        if parsed is None or not self._valid_run_id(run_id):
            logger.warning(
                "automations: invalid cooldown config; gate skipped "
                "(automation_id=%s tenant_id=%s)",
                message.automation_id,
                message.tenant_id,
            )
            return self._decide(
                CooldownOutcome.FAIL_OPEN,
                reason=REASON_INVALID_CONFIG,
            )

        fields, seconds, scheme_ver = parsed
        try:
            entity_hash = canonical_cooldown_hash(message.alert, fields)
        except MissingCooldownField as exc:
            logger.warning(
                "automations: cooldown field missing; gate skipped "
                "(automation_id=%s tenant_id=%s missing_count=%s)",
                message.automation_id,
                message.tenant_id,
                len(exc.fields),
            )
            return self._decide(
                CooldownOutcome.MISSING_FIELD,
                missing_fields=exc.fields,
                reason=REASON_MISSING_FIELD,
            )
        except (TypeError, ValueError):
            logger.warning(
                "automations: invalid cooldown fields; gate skipped "
                "(automation_id=%s tenant_id=%s)",
                message.automation_id,
                message.tenant_id,
            )
            return self._decide(
                CooldownOutcome.FAIL_OPEN,
                reason=REASON_INVALID_CONFIG,
            )

        key = cooldown_key(message.automation_id, scheme_ver, entity_hash)
        try:
            client = self._client_factory()
            if client is None:
                return self._decide(
                    CooldownOutcome.FAIL_OPEN,
                    key=key,
                    entity_hash=entity_hash,
                    cooldown_seconds=seconds,
                    reason=REASON_REDIS_UNAVAILABLE,
                )
            claimed = client.set(
                key,
                run_id,
                nx=True,
                ex=COOLDOWN_PROVISIONAL_TTL_SECONDS,
            )
        except Exception:  # noqa: BLE001 - a gate must never drop the message
            self._redis_failed(RedisOperation.COOLDOWN_CLAIM)
            return self._decide(
                CooldownOutcome.FAIL_OPEN,
                key=key,
                entity_hash=entity_hash,
                cooldown_seconds=seconds,
                reason=REASON_REDIS_UNAVAILABLE,
            )

        if claimed:
            return self._decide(
                CooldownOutcome.CLAIMED,
                key=key,
                entity_hash=entity_hash,
                cooldown_seconds=seconds,
                owner_token=run_id,
            )

        # Losing NX is itself authoritative for this best-effort gate.  TTL is
        # fetched only to render suppression metadata and never changes the
        # suppress decision or refreshes the winner's key.
        remaining: Optional[int] = None
        try:
            ttl = client.ttl(key)
            if ttl >= 0:
                remaining = ttl
        except Exception:  # noqa: BLE001 - metadata failure must not unsuppress
            self._redis_failed(RedisOperation.COOLDOWN_TTL)

        next_eligible = None
        if remaining is not None:
            next_eligible = datetime.fromtimestamp(
                self._wall_clock(), tz=timezone.utc
            ) + timedelta(seconds=remaining)

        return self._decide(
            CooldownOutcome.SUPPRESSED,
            key=key,
            entity_hash=entity_hash,
            cooldown_seconds=seconds,
            eligible_again_in_seconds=remaining,
            next_eligible_at=next_eligible,
        )

    def activate(self, decision: CooldownDecision) -> bool:
        """Arm the full cooldown only if this decision still owns the key.

        Arming rotates the value from the run_id to ``COOLDOWN_ARMED_VALUE``,
        retiring the ownership token: once armed, neither this decision's
        ``release`` nor a stale claimant can extend or delete the claim.
        """
        if not self._has_owned_claim(decision):
            return False
        assert decision.key is not None
        assert decision.owner_token is not None
        assert decision.cooldown_seconds is not None
        try:
            client = self._client_factory()
            if client is None:
                return False
            return self._lifecycle.activate(
                client,
                key=decision.key,
                owner_token=decision.owner_token,
                cooldown_seconds=decision.cooldown_seconds,
            )
        except Exception:  # noqa: BLE001
            self._redis_failed(RedisOperation.COOLDOWN_ACTIVATE)
            return False

    def release(self, decision: CooldownDecision) -> bool:
        """Delete the provisional key only if this decision still owns it."""
        if not self._has_owned_claim(decision):
            return False
        assert decision.key is not None
        assert decision.owner_token is not None
        try:
            client = self._client_factory()
            if client is None:
                return False
            return self._lifecycle.release(
                client,
                key=decision.key,
                owner_token=decision.owner_token,
            )
        except Exception:  # noqa: BLE001
            self._redis_failed(RedisOperation.COOLDOWN_RELEASE)
            return False

    @staticmethod
    def _parse_config(config: object) -> Optional[tuple[list[str], int, int]]:
        if not isinstance(config, dict):
            return None
        fields = config.get("fields")
        seconds = config.get("seconds")
        scheme_ver = config.get("scheme_ver")
        if not isinstance(fields, list) or any(
            not isinstance(field, str) for field in fields
        ):
            return None
        if (
            not isinstance(seconds, int)
            or isinstance(seconds, bool)
            or seconds < 1
        ):
            return None
        if (
            not isinstance(scheme_ver, int)
            or isinstance(scheme_ver, bool)
            or scheme_ver != COOLDOWN_SCHEME_VER
        ):
            return None
        return fields, seconds, scheme_ver

    @staticmethod
    def _valid_run_id(run_id: object) -> bool:
        if not isinstance(run_id, str):
            return False
        try:
            UUID(run_id)
        except (ValueError, AttributeError):
            return False
        return True

    @staticmethod
    def _has_owned_claim(decision: CooldownDecision) -> bool:
        return (
            decision.outcome is CooldownOutcome.CLAIMED
            and decision.key is not None
            and decision.owner_token is not None
            and decision.cooldown_seconds is not None
        )

    def _redis_failed(self, operation: RedisOperation) -> None:
        redis_error_counters[operation].inc()
        now = self._monotonic_clock()
        with self._log_lock:
            should_log = now >= self._next_error_log_at
            if should_log:
                self._next_error_log_at = now + self._error_log_interval_seconds
        if should_log:
            logger.exception(
                "automations: cooldown Redis %s failed; gate remains fail-open "
                "(further tracebacks suppressed for %ss)",
                operation.value,
                self._error_log_interval_seconds,
            )

    @staticmethod
    def _decide(
        outcome: CooldownOutcome,
        *,
        key: Optional[str] = None,
        entity_hash: Optional[str] = None,
        cooldown_seconds: Optional[int] = None,
        owner_token: Optional[str] = None,
        eligible_again_in_seconds: Optional[int] = None,
        next_eligible_at: Optional[datetime] = None,
        missing_fields: tuple[str, ...] = (),
        reason: Optional[str] = None,
    ) -> CooldownDecision:
        cooldown_decision_counters[outcome].inc()
        flags = {"cooldown": "skipped" if reason else outcome.value}
        if reason:
            flags["reason"] = reason
        if missing_fields:
            flags["missing_fields"] = ",".join(missing_fields)
        return CooldownDecision(
            outcome=outcome,
            key=key,
            entity_hash=entity_hash,
            cooldown_seconds=cooldown_seconds,
            owner_token=owner_token,
            eligible_again_in_seconds=eligible_again_in_seconds,
            next_eligible_at=next_eligible_at,
            missing_fields=missing_fields,
            gate_flags=flags,
        )
