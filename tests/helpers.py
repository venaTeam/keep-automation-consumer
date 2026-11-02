"""Shared explicit dependencies for consumer unit tests."""

from concurrent.futures import Future
from typing import Any, Callable, Iterable, Optional

from src.bl.gates.cooldown import CooldownGate
from src.bl.gates.idempotency import IdempotencyGate
from src.bl.pipeline import SubmitPipeline
from src.bl.suppression_audit import LoggingSuppressionAuditor, SuppressionAuditor
from src.core.kafka_consumer import MatchedAlertConsumer
from src.core.offset_tracker import OffsetTracker
from src.models.submit import (
    SubmitOutcome,
    SubmitRequest,
    SubmitResponse,
    SubmitResult,
    SubmitStatus,
)


class InlineWorkerPool:
    def has_capacity(self) -> bool:
        return True

    def try_submit(
        self, function: Callable[..., Any], /, *args: Any, **kwargs: Any
    ) -> bool:
        future: Future[Any] = Future()
        try:
            future.set_result(function(*args, **kwargs))
        except BaseException as exc:
            future.set_exception(exc)
        return True

    def shutdown(self, *, wait: bool = True) -> None:
        pass


class InMemoryRedis:
    """Enough Redis for both gates: SET [NX] [EX], GET, TTL, and the cooldown
    Lua scripts (compare-and-rotate / compare-and-delete), plus failure
    injection by operation name."""

    def __init__(self, fail_on: Iterable[str] = ()):
        self.values: dict[str, str] = {}
        self.ttls: dict[str, Optional[int]] = {}
        self.fail_on = set(fail_on)
        self.calls: list[tuple] = []

    def _check(self, operation: str) -> None:
        self.calls.append((operation,))
        if operation in self.fail_on or "*" in self.fail_on:
            raise ConnectionError("redis down")

    def set(self, key, value, nx=False, ex=None):
        self._check("set")
        if nx and key in self.values:
            return None
        self.values[key] = value
        self.ttls[key] = ex
        return True

    def get(self, key):
        self._check("get")
        return self.values.get(key)

    def ttl(self, key):
        self._check("ttl")
        if key not in self.values:
            return -2
        return self.ttls.get(key) or -1

    def register_script(self, script):
        self._check("register_script")
        operation = "release" if "DEL" in script else "activate"

        def execute(*, keys, args, client=None):
            del client
            self._check(operation)
            key = keys[0]
            if self.values.get(key) != args[0]:
                return 0
            if operation == "activate":
                self.values[key] = args[1]
                self.ttls[key] = int(args[2])
                return 1
            del self.values[key]
            self.ttls.pop(key, None)
            return 1

        return execute


class RecordingAuditor:
    """Explicit signature on purpose: a `**row` fake would accept a renamed
    parameter that the real `LoggingSuppressionAuditor` rejects."""

    def __init__(self):
        self.rows: list[dict] = []

    def record_suppression(
        self,
        *,
        tenant_id,
        automation_id,
        history_id,
        fingerprint,
        reason,
        gate_flags=None,
        detail=None,
    ):
        self.rows.append(
            {
                "tenant_id": tenant_id,
                "automation_id": automation_id,
                "history_id": history_id,
                "fingerprint": fingerprint,
                "reason": reason,
                "gate_flags": gate_flags,
                "detail": detail,
            }
        )


def acknowledged(status: SubmitStatus, run_id: str = "server-run-id") -> SubmitResult:
    outcome = SubmitOutcome(status.value)
    return SubmitResult(
        outcome,
        http_status=200,
        response=SubmitResponse(status=status, run_id=run_id),
    )


ACCEPTED = acknowledged(SubmitStatus.ACCEPTED)


def failure(outcome: SubmitOutcome, http_status: Optional[int] = None) -> SubmitResult:
    return SubmitResult(outcome, http_status=http_status, error=outcome.value)


class ScriptedSubmitClient:
    """Returns scripted results in order (the last one repeats) and records
    every request. `on_submit` runs before answering — e.g. to inspect Redis
    mid-flight or to stop the consumer."""

    def __init__(
        self,
        *results: SubmitResult,
        on_submit: Optional[Callable[[SubmitRequest], None]] = None,
    ):
        self._results = list(results) or [ACCEPTED]
        self._on_submit = on_submit
        self.requests: list[SubmitRequest] = []
        self.closed = False

    def submit(self, request: SubmitRequest) -> SubmitResult:
        self.requests.append(request)
        if self._on_submit is not None:
            self._on_submit(request)
        if len(self._results) > 1:
            return self._results.pop(0)
        return self._results[0]

    def close(self) -> None:
        self.closed = True


def build_test_pipeline(
    *,
    redis: Any = None,
    idempotency_gate: Optional[IdempotencyGate] = None,
    cooldown_gate: Optional[CooldownGate] = None,
    submit_client: Any = None,
    suppression_auditor: Optional[SuppressionAuditor] = None,
    backoff_initial_seconds: float = 0.0,
    backoff_max_seconds: float = 0.0,
) -> SubmitPipeline:
    return SubmitPipeline(
        idempotency_gate=(
            idempotency_gate
            if idempotency_gate is not None
            else IdempotencyGate(client_factory=lambda: redis)
        ),
        cooldown_gate=(
            cooldown_gate
            if cooldown_gate is not None
            else CooldownGate(client_factory=lambda: redis, error_log_interval_seconds=0)
        ),
        submit_client=submit_client if submit_client is not None else ScriptedSubmitClient(),
        suppression_auditor=(
            suppression_auditor
            if suppression_auditor is not None
            else LoggingSuppressionAuditor()
        ),
        backoff_initial_seconds=backoff_initial_seconds,
        backoff_max_seconds=backoff_max_seconds,
        retry_log_interval_seconds=0,
    )


def build_test_consumer(
    *,
    pipeline: Optional[SubmitPipeline] = None,
    idempotency_gate: Optional[IdempotencyGate] = None,
    suppression_auditor: Optional[SuppressionAuditor] = None,
    submit_client: Any = None,
    worker_pool: Any = None,
    offset_tracker: Optional[OffsetTracker] = None,
    monotonic_clock: Optional[Callable[[], float]] = None,
) -> MatchedAlertConsumer:
    if pipeline is None:
        pipeline = build_test_pipeline(
            idempotency_gate=idempotency_gate,
            suppression_auditor=suppression_auditor,
            submit_client=submit_client,
        )
    kwargs = {}
    if monotonic_clock is not None:
        kwargs["monotonic_clock"] = monotonic_clock
    return MatchedAlertConsumer(
        pipeline=pipeline,
        worker_pool=worker_pool if worker_pool is not None else InlineWorkerPool(),
        offset_tracker=offset_tracker,
        **kwargs,
    )
