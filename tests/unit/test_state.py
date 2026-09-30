from __future__ import annotations

from agent_bus.models import AckEvent, DeliveredEvent, Event, HumanTurnEvent, MessageEvent
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


def test_turns_count_both_directions_since_either_human_turn() -> None:
    events: list[Event] = [
        _m("1", "alice", "bob"),
        _m("2", "bob", "alice"),
        HumanTurnEvent(agent="bob", session="s"),
        _m("3", "alice", "bob"),
        _m("4", "bob", "alice"),
        _m("5", "alice", "carol"),
    ]
    state = BusState.from_events(events)
    assert state.agent_turns_since_human("alice", "bob") == 2
    assert state.agent_turns_since_human("bob", "alice") == 2
    assert state.agent_turns_since_human("alice", "carol") == 1


def test_a_third_agents_human_turn_does_not_reset_the_pair() -> None:
    events: list[Event] = [
        _m("1", "alice", "bob"),
        HumanTurnEvent(agent="carol", session="s"),
        _m("2", "bob", "alice"),
    ]
    assert BusState.from_events(events).agent_turns_since_human("alice", "bob") == 2


def test_duplicate_message_ids_keep_the_first() -> None:
    first = _m("A", "alice", "bob")
    dup = MessageEvent(id="A", sender="carol", to="bob", body="dup", thread="A")
    state = BusState.from_events([first, dup])
    assert state.messages["A"].sender == "alice"
