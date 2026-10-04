from __future__ import annotations

from datetime import timedelta

from agent_bus.models import (
    AckEvent,
    DeliveredEvent,
    Event,
    HumanTurnEvent,
    MessageEvent,
    now,
)
from agent_bus.state import BusState


def _m(msg_id: str, sender: str, to: str) -> MessageEvent:
    return MessageEvent(id=msg_id, sender=sender, to=to, body=msg_id, thread=msg_id)


def test_unread_and_undelivered_follow_their_events() -> None:
    state = BusState.from_events(
        [
            _m("A", "alice", "bob"),
            _m("B", "alice", "bob"),
            _m("C", "bob", "alice"),
            DeliveredEvent(id="A", to="bob", session="s"),
            AckEvent(id="B", by="bob"),
        ]
    )
    assert [m.id for m in state.inbox("bob")] == ["A", "B"]
    assert [m.id for m in state.unread("bob")] == ["A"]
    assert [m.id for m in state.undelivered("bob")] == ["B"]
    assert [m.id for m in state.unread("alice")] == ["C"]
    assert state.agents() == {"alice", "bob"}


def test_order_is_log_position_not_timestamp() -> None:
    late = _m("LATE", "alice", "bob")
    early = MessageEvent(
        id="EARLY", sender="alice", to="bob", body="x", thread="x", ts=late.ts.replace(year=2000)
    )
    state = BusState.from_events([late, early])
    assert [m.id for m in state.inbox("bob")] == ["LATE", "EARLY"]


def _at(msg_id: str, sender: str, to: str, minutes_ago: float) -> MessageEvent:
    ts = now() - timedelta(minutes=minutes_ago)
    return MessageEvent(id=msg_id, sender=sender, to=to, body=msg_id, thread=msg_id, ts=ts)


def test_messages_between_counts_the_pair_both_ways_within_the_window() -> None:
    events: list[Event] = [
        _at("old", "alice", "bob", 10),
        _at("1", "alice", "bob", 2),
        _at("2", "bob", "alice", 1),
        HumanTurnEvent(agent="bob", session="s"),
        _at("3", "alice", "carol", 1),
    ]
    state = BusState.from_events(events)
    since = now() - timedelta(minutes=5)
    assert state.messages_between("alice", "bob", since=since) == 2
    assert state.messages_between("bob", "alice", since=since) == 2
    assert state.messages_between("alice", "carol", since=since) == 1


def test_duplicate_message_ids_keep_the_first() -> None:
    first = _m("A", "alice", "bob")
    dup = MessageEvent(id="A", sender="carol", to="bob", body="dup", thread="A")
    state = BusState.from_events([first, dup])
    assert state.messages["A"].sender == "alice"
