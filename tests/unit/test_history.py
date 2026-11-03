from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from agent_bus.bus import Bus, BusError, parse_since
from agent_bus.models import MessageEvent, now
from agent_bus.store import Store


def test_history_holds_both_directions_oldest_first(alice: Bus, bob: Bus) -> None:
    one = alice.send("bob", "one")
    two = bob.send("alice", "two")
    assert [m.id for m in alice.history()] == [one.id, two.id]


def test_history_filters_by_sender_thread_and_last(alice: Bus, bob: Bus, store: Store) -> None:
    carol = Bus(store, "carol")
    task = alice.send("bob", "task", kind="task")
    carol.send("bob", "from carol")
    answer = bob.send("alice", "answer", kind="result", reply_to=task.id)
    assert [m.body for m in bob.history(sender="carol")] == ["from carol"]
    assert [m.id for m in bob.history(thread=task.thread)] == [task.id, answer.id]
    assert [m.id for m in bob.history(last=1)] == [answer.id]
    with pytest.raises(BusError, match="at least 1"):
        bob.history(last=0)


def test_history_since_compares_timestamps(store: Store, alice: Bus) -> None:
    store.append(
        MessageEvent(sender="bob", to="alice", body="old", thread="T", ts=now() - timedelta(days=2))
    )
    alice.send("bob", "new")
    assert [m.body for m in alice.history(since=now() - timedelta(hours=1))] == ["new"]
    assert len(alice.history(last=None)) == 2


def test_history_is_capped_by_default(store: Store) -> None:
    bus = Bus(store, "alice", rate_limit=1000)
    for i in range(25):
        bus.send("bob", f"m{i}")
    found = bus.history()
    assert (len(found), found[0].body) == (20, "m5")


def test_since_reads_ages_and_iso_times() -> None:
    assert abs(parse_since("2h") - (now() - timedelta(hours=2))) < timedelta(seconds=5)
    assert parse_since("2026-11-03T09:00:00Z") == datetime(2026, 11, 3, 9, tzinfo=UTC)
    assert parse_since("2026-11-03T09:00:00") == datetime(2026, 11, 3, 9, tzinfo=UTC)
    with pytest.raises(BusError, match="neither ISO-8601"):
        parse_since("yesterday")
