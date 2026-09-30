from __future__ import annotations

import json
import time

import pytest
from pydantic import ValidationError

from agent_bus.models import EVENT_ADAPTER, MessageEvent, check_agent_name, dump_event, new_id


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
