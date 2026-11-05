from __future__ import annotations

import json
import time
from collections.abc import Callable

import pytest
from pydantic import ValidationError

from agent_bus.bus import BusError
from agent_bus.models import (
    EVENT_ADAPTER,
    AckEvent,
    DeliveredEvent,
    HumanTurnEvent,
    MessageEvent,
    check_agent_name,
    check_team_name,
    check_thread_post,
    dump_event,
    new_id,
    participants,
)


def test_ids_are_26_crockford_chars_and_sort_by_time() -> None:
    first = new_id()
    time.sleep(0.002)
    second = new_id()
    assert len(first) == 26
    assert set(first) <= set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")
    assert first < second


def test_ids_are_unique_within_a_millisecond() -> None:
    assert len({new_id() for _ in range(1000)}) == 1000


@pytest.mark.parametrize("name", ["a", "alice", "agent-2", "x_y", "a" * 32])
def test_valid_agent_names(name: str) -> None:
    assert check_agent_name(name) == name


@pytest.mark.parametrize("name", ["", "Alice", "-a", "a b", "a/b", "a" * 33, "é"])
def test_invalid_agent_names(name: str) -> None:
    with pytest.raises(ValueError, match="invalid agent name"):
        check_agent_name(name)


def test_message_line_uses_from_and_round_trips() -> None:
    msg = MessageEvent(sender="alice", to="bob", body="привет\nмир", thread="T")  # noqa: RUF001
    line = dump_event(msg)
    assert line.endswith(b"\n")
    assert line.count(b"\n") == 1
    raw = json.loads(line)
    assert raw["from"] == "alice"
    assert "sender" not in raw
    assert "reply_to" not in raw
    assert EVENT_ADAPTER.validate_json(line) == msg


def test_unknown_event_type_is_rejected() -> None:
    with pytest.raises(ValidationError):
        EVENT_ADAPTER.validate_json(b'{"type": "future", "v": 2}')


def test_message_rejects_invalid_recipient() -> None:
    with pytest.raises(ValidationError):
        MessageEvent(sender="alice", to="Bob!", body="x", thread="T")


def test_seq_is_left_out_of_local_lines_and_kept_from_hub_lines() -> None:
    msg = MessageEvent(sender="alice", to="bob", body="x", thread="T")
    assert "seq" not in json.loads(dump_event(msg))
    from_hub = EVENT_ADAPTER.validate_json(dump_event(msg.model_copy(update={"seq": 42})))
    assert from_hub.seq == 42


def test_participants_of_a_message_are_its_two_parties() -> None:
    msg = MessageEvent(sender="alice", to="bob", body="x", thread="T")
    assert participants(msg) == {"alice", "bob"}


@pytest.mark.parametrize(
    "make",
    [
        lambda msg_id: AckEvent(id=msg_id, by="bob"),
        lambda msg_id: DeliveredEvent(id=msg_id, to="bob", session="S"),
    ],
)
def test_participants_of_a_settlement_are_those_of_its_message(
    make: Callable[[str], AckEvent | DeliveredEvent],
) -> None:
    msg = MessageEvent(sender="alice", to="bob", body="x", thread="T")
    event = make(msg.id)
    assert participants(event, msg) == {"alice", "bob"}
    with pytest.raises(ValueError, match="needs the message"):
        participants(event)
    other = MessageEvent(sender="carol", to="bob", body="y", thread="T")
    with pytest.raises(ValueError, match="needs the message"):
        participants(event, other)


def test_human_turn_belongs_to_its_agent_alone() -> None:
    assert participants(HumanTurnEvent(agent="bob", session="S")) == {"bob"}


def _root() -> MessageEvent:
    return MessageEvent(id="R", sender="alice", to="bob", kind="result", body="r", thread="R")


_OTHER_PARTICIPANT = "recipient of a thread message must be the thread's other participant"


@pytest.mark.parametrize(
    ("root", "thread", "sender", "to", "match"),
    [
        (None, "R", "alice", "bob", "unknown thread: R"),
        (_root(), "OTHER", "alice", "bob", "unknown thread: OTHER"),
        (
            MessageEvent(id="R", sender="alice", to="bob", body="r", thread="NOT-R"),
            "R",
            "alice",
            "bob",
            "unknown thread: R",
        ),
        (_root(), "R", "carol", "bob", "not a participant of thread R"),
        (_root(), "R", "carol", "alice", "not a participant of thread R"),
        (_root(), "R", "alice", "carol", _OTHER_PARTICIPANT),
        (_root(), "R", "alice", "alice", _OTHER_PARTICIPANT),
        (_root(), "R", "bob", "bob", _OTHER_PARTICIPANT),
        (_root(), "R", "bob", "carol", _OTHER_PARTICIPANT),
    ],
)
def test_check_thread_post_refuses(
    root: MessageEvent | None, thread: str, sender: str, to: str, match: str
) -> None:
    with pytest.raises(BusError, match=match):
        check_thread_post(root, thread, sender, to)


def test_check_thread_post_prefixes_are_exact() -> None:
    with pytest.raises(BusError) as unknown:
        check_thread_post(None, "T", "alice", "bob")
    assert str(unknown.value).startswith("unknown thread: T")
    with pytest.raises(BusError) as stranger:
        check_thread_post(_root(), "R", "carol", "bob")
    assert str(stranger.value).startswith("not a participant of thread R")
    with pytest.raises(BusError) as wrong:
        check_thread_post(_root(), "R", "alice", "carol")
    assert str(wrong.value).startswith(
        "the recipient of a thread message must be the thread's other participant"
    )


def test_check_thread_post_accepts_both_directions_whatever_the_root_kind() -> None:
    root = _root()
    for kind in ("task", "text"):
        check_thread_post(root, "R", "alice", "bob", kind)
        check_thread_post(root, "R", "bob", "alice", kind)


@pytest.mark.parametrize("kind", ["result", "await_human"])
def test_check_thread_post_refuses_a_result_and_a_handoff(kind: str) -> None:
    with pytest.raises(BusError, match="a thread post must be kind task or text"):
        check_thread_post(_root(), "R", "alice", "bob", kind)


def test_team_names_follow_the_agent_name_rule() -> None:
    assert check_team_name("core-2") == "core-2"
    with pytest.raises(ValueError, match="invalid team name"):
        check_team_name("Core Team")
