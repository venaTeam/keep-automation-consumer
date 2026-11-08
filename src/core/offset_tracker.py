"""Per-partition contiguous commit positions for out-of-order workers.

Workers settle messages in any order, but a Kafka commit of offset N means
"everything below N is done". Committing a later message's offset while an
earlier one is still retrying would silently skip the earlier one on the next
restart. So each partition's commit position is the **lowest offset still
outstanding** (dispatched, not settled) — or, with nothing outstanding, one past
the highest offset dispatched.

Only outstanding offsets are stored, so memory is bounded by in-flight work (the
worker pool), not by how far a partition has run ahead of a stuck message. That
distance — the *uncommitted window* — is reported so the consumer can pause a
partition whose window is full.

Each assignment of a partition gets a fresh **epoch**. A worker settles with the
epoch it was dispatched under; after a revoke (and possibly a re-assign) a stale
worker's settle is ignored, so it can never move the new assignment's position.

Thread-safe: the poll thread tracks/commits, worker threads settle.
"""

import threading
from dataclasses import dataclass, field
from typing import Iterable, Optional

PartitionKey = tuple[str, int]  # (topic, partition)


@dataclass
class _PartitionState:
    epoch: int
    outstanding: set[int] = field(default_factory=set)
    max_dispatched: Optional[int] = None
    last_committed: Optional[int] = None

    def position(self) -> Optional[int]:
        if self.outstanding:
            return min(self.outstanding)
        if self.max_dispatched is None:
            return None
        return self.max_dispatched + 1

    def window(self) -> int:
        if not self.outstanding or self.max_dispatched is None:
            return 0
        return self.max_dispatched - min(self.outstanding) + 1


class OffsetTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._partitions: dict[PartitionKey, _PartitionState] = {}
        self._next_epoch = 1

    def assign(self, keys: Iterable[PartitionKey]) -> None:
        """Start fresh state for newly assigned partitions."""
        with self._lock:
            for key in keys:
                self._partitions[key] = self._new_state()

    def revoke(self, keys: Iterable[PartitionKey]) -> None:
        """Forget partitions this consumer no longer owns."""
        with self._lock:
            for key in keys:
                self._partitions.pop(key, None)

    def track(self, key: PartitionKey, offset: int) -> int:
        """Record an offset as outstanding; returns the epoch to settle it with.

        Called for a dispatched message, and also for a polled message that is
        rewound instead of dispatched: until it comes back and settles, the
        commit position cannot move past it. Re-tracking is idempotent.

        A partition seen without an `assign` (a client that never ran the
        rebalance callback) gets state on first sight rather than failing.
        """
        with self._lock:
            state = self._partitions.get(key)
            if state is None:
                state = self._partitions[key] = self._new_state()
            state.outstanding.add(offset)
            if state.max_dispatched is None or offset > state.max_dispatched:
                state.max_dispatched = offset
            return state.epoch

    def settle(self, key: PartitionKey, offset: int, epoch: int) -> bool:
        """Mark a dispatched offset finished. False when it is stale."""
        with self._lock:
            state = self._partitions.get(key)
            if state is None or state.epoch != epoch:
                return False
            state.outstanding.discard(offset)
            return True

    def owns(self, key: PartitionKey, epoch: int) -> bool:
        with self._lock:
            state = self._partitions.get(key)
            return state is not None and state.epoch == epoch

    def commits_due(
        self,
        keys: Optional[Iterable[PartitionKey]] = None,
        *,
        include_committed: bool = False,
    ) -> list[tuple[PartitionKey, int]]:
        """Positions that moved past the last commit (all partitions, or `keys`).

        `include_committed` also returns positions already committed — for a
        final synchronous commit that must not trust an async one in flight.
        The caller commits them and then calls `mark_committed`.
        """
        with self._lock:
            selected = self._partitions if keys is None else {
                key: self._partitions[key] for key in keys if key in self._partitions
            }
            due = []
            for key, state in selected.items():
                position = state.position()
                if position is not None and (
                    include_committed or position != state.last_committed
                ):
                    due.append((key, position))
            return due

    def mark_committed(self, commits: Iterable[tuple[PartitionKey, int]]) -> None:
        with self._lock:
            for key, position in commits:
                state = self._partitions.get(key)
                if state is not None:
                    state.last_committed = position

    def mark_commit_failed(self, keys: Iterable[PartitionKey]) -> None:
        """Make the next `commits_due` re-offer these partitions' positions."""
        with self._lock:
            for key in keys:
                state = self._partitions.get(key)
                if state is not None:
                    state.last_committed = None

    def windows_at_least(self, limit: int) -> set[PartitionKey]:
        """Partitions whose uncommitted window has reached `limit`."""
        with self._lock:
            return {
                key
                for key, state in self._partitions.items()
                if state.window() >= limit
            }

    def _new_state(self) -> _PartitionState:
        epoch = self._next_epoch
        self._next_epoch += 1
        return _PartitionState(epoch=epoch)
