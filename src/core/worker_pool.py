"""Bounded worker pool for per-message processing outside the Kafka poll loop."""

from concurrent.futures import Future, ThreadPoolExecutor
from threading import BoundedSemaphore
from typing import Any, Callable, Protocol, TypeVar

from src.core.metrics import worker_pool_tasks_in_flight


T = TypeVar("T")


class WorkerPool(Protocol):
    def has_capacity(self) -> bool: ...

    def try_submit(
        self, function: Callable[..., T], /, *args: Any, **kwargs: Any
    ) -> bool: ...

    def shutdown(self, *, wait: bool = True) -> None: ...


class BoundedWorkerPool:
    """Executor with no unbounded work queue.

    Admission is nonblocking. Kafka pauses assigned partitions and keeps polling
    when no slot is available, so worker saturation cannot starve heartbeats or
    grow an unbounded executor queue.
    """

    def __init__(self, max_workers: int) -> None:
        self._slots = BoundedSemaphore(max_workers)
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="automation-message",
        )

    def has_capacity(self) -> bool:
        acquired = self._slots.acquire(blocking=False)
        if acquired:
            self._slots.release()
        return acquired

    def try_submit(
        self, function: Callable[..., T], /, *args: Any, **kwargs: Any
    ) -> bool:
        if not self._slots.acquire(blocking=False):
            return False
        worker_pool_tasks_in_flight.inc()
        try:
            future = self._executor.submit(function, *args, **kwargs)
        except BaseException:
            self._slots.release()
            worker_pool_tasks_in_flight.dec()
            raise
        future.add_done_callback(self._release_slot)
        return True

    def _release_slot(self, _: Future[Any]) -> None:
        worker_pool_tasks_in_flight.dec()
        self._slots.release()

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)
