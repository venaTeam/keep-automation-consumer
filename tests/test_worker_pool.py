"""Bounded worker-pool behavior."""

import threading

from src.core.worker_pool import BoundedWorkerPool


def test_submission_is_rejected_without_blocking_when_all_slots_are_occupied():
    release_first = threading.Event()
    first_started = threading.Event()
    pool = BoundedWorkerPool(max_workers=1)

    def first_task():
        first_started.set()
        release_first.wait(timeout=2)

    assert pool.try_submit(first_task) is True
    assert first_started.wait(timeout=1)
    assert pool.has_capacity() is False
    assert pool.try_submit(lambda: None) is False

    release_first.set()
    pool.shutdown(wait=True)


def test_capacity_returns_after_task_completion():
    finished = threading.Event()
    pool = BoundedWorkerPool(max_workers=1)

    assert pool.try_submit(lambda: finished.set()) is True
    assert finished.wait(timeout=1)

    assert pool.has_capacity() is True
    pool.shutdown(wait=True)
