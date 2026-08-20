"""Shared explicit dependencies for consumer unit tests."""

from concurrent.futures import Future
from typing import Any, Callable

from src.bl.gates.idempotency import IdempotencyGate
from src.bl.suppression_audit import LoggingSuppressionAuditor, SuppressionAuditor
from src.core.kafka_consumer import MatchedAlertConsumer


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


def build_test_consumer(
    *,
    idempotency_gate: IdempotencyGate | None = None,
    suppression_auditor: SuppressionAuditor | None = None,
    worker_pool: Any = None,
) -> MatchedAlertConsumer:
    gate = (
        idempotency_gate
        if idempotency_gate is not None
        else IdempotencyGate(client_factory=lambda: None)
    )
    auditor = (
        suppression_auditor
        if suppression_auditor is not None
        else LoggingSuppressionAuditor()
    )
    pool = worker_pool if worker_pool is not None else InlineWorkerPool()
    return MatchedAlertConsumer(
        idempotency_gate=gate,
        suppression_auditor=auditor,
        worker_pool=pool,
    )
