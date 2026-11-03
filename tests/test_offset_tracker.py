"""Contiguous per-partition commit positions (C11)."""

import threading

from src.core.offset_tracker import OffsetTracker

P0 = ("matched-alerts", 0)
P1 = ("matched-alerts", 1)


def _dispatch(tracker, key, *offsets):
    return {offset: tracker.track(key, offset) for offset in offsets}


def test_position_is_one_past_the_highest_settled_contiguous_offset():
    tracker = OffsetTracker()
    epochs = _dispatch(tracker, P0, 10, 11, 12)
    for offset, epoch in epochs.items():
        tracker.settle(P0, offset, epoch)

    assert tracker.commits_due() == [(P0, 13)]


def test_a_gap_holds_the_position_at_the_lowest_outstanding_offset():
    tracker = OffsetTracker()
    epochs = _dispatch(tracker, P0, 10, 11, 12)
    tracker.settle(P0, 11, epochs[11])
    tracker.settle(P0, 12, epochs[12])

    assert tracker.commits_due() == [(P0, 10)]

    tracker.settle(P0, 10, epochs[10])
    assert tracker.commits_due() == [(P0, 13)]


def test_offset_gaps_in_the_log_do_not_block_commits():
    """Compacted topics / transaction markers skip offsets; only dispatched
    offsets are outstanding."""
    tracker = OffsetTracker()
    epochs = _dispatch(tracker, P0, 3, 7, 20)
    for offset, epoch in epochs.items():
        tracker.settle(P0, offset, epoch)

    assert tracker.commits_due() == [(P0, 21)]


def test_partitions_are_independent():
    tracker = OffsetTracker()
    _dispatch(tracker, P0, 0)  # stuck
    p1 = _dispatch(tracker, P1, 0, 1)
    for offset, epoch in p1.items():
        tracker.settle(P1, offset, epoch)

    assert sorted(tracker.commits_due()) == [(P0, 0), (P1, 2)]


def test_committed_positions_are_not_offered_again_until_they_move():
    tracker = OffsetTracker()
    epochs = _dispatch(tracker, P0, 0)
    tracker.settle(P0, 0, epochs[0])
    tracker.mark_committed(tracker.commits_due())

    assert tracker.commits_due() == []

    tracker.settle(P0, 1, tracker.track(P0, 1))
    assert tracker.commits_due() == [(P0, 2)]


def test_commits_due_can_be_limited_to_some_partitions():
    tracker = OffsetTracker()
    tracker.settle(P0, 0, tracker.track(P0, 0))
    tracker.settle(P1, 0, tracker.track(P1, 0))

    assert tracker.commits_due([P1]) == [(P1, 1)]
    assert tracker.commits_due([("other", 9)]) == []


def test_window_spans_from_the_stuck_offset_to_the_newest_dispatch():
    tracker = OffsetTracker()
    epochs = _dispatch(tracker, P0, 100, 101, 102, 103)
    for offset in (101, 102, 103):
        tracker.settle(P0, offset, epochs[offset])
    _dispatch(tracker, P1, 0)

    assert tracker.windows_at_least(4) == {P0}
    assert tracker.windows_at_least(5) == set()

    tracker.settle(P0, 100, epochs[100])
    assert tracker.windows_at_least(1) == {P1}


def test_revoke_forgets_state_and_reassign_starts_a_new_epoch():
    tracker = OffsetTracker()
    old_epoch = tracker.track(P0, 5)
    tracker.revoke([P0])

    assert tracker.owns(P0, old_epoch) is False
    assert tracker.commits_due() == []

    tracker.assign([P0])
    new_epoch = tracker.track(P0, 5)
    assert new_epoch != old_epoch
    assert tracker.settle(P0, 5, old_epoch) is False
    assert tracker.settle(P0, 5, new_epoch) is True


def test_rewound_offset_retracked_is_idempotent():
    tracker = OffsetTracker()
    tracker.track(P0, 9)  # rewound, not dispatched
    epoch = tracker.track(P0, 9)  # redelivered and dispatched

    assert tracker.commits_due() == [(P0, 9)]
    tracker.settle(P0, 9, epoch)
    assert tracker.commits_due() == [(P0, 10)]


def test_concurrent_settles_are_safe():
    tracker = OffsetTracker()
    epochs = _dispatch(tracker, P0, *range(2000))

    def settle(offsets):
        for offset in offsets:
            tracker.settle(P0, offset, epochs[offset])

    threads = [
        threading.Thread(target=settle, args=(range(i, 2000, 8),)) for i in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert tracker.commits_due() == [(P0, 2000)]
