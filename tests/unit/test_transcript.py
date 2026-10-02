from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from agent_bus.models import new_id
from agent_bus.names import CHANNEL_SOURCE
from agent_bus.transcript import Transcript, channel_ids


def channel_record(msg_id: str, source: str = CHANNEL_SOURCE) -> dict[str, Any]:
    """A channel event as Claude Code 2.1.285 wrote it to the transcript (stage 0)."""
    return {
        "type": "user",
        "origin": {"kind": "channel", "server": source},
        "isMeta": True,
        "message": {
            "role": "user",
            "content": (
                f'<channel source="{source}" from="alice" msg_id="{msg_id}">\nhi\n</channel>'
            ),
        },
    }


def test_a_channel_record_yields_its_msg_id() -> None:
    msg_id = new_id()
    assert channel_ids(channel_record(msg_id)) == {msg_id}


def test_list_content_is_read_too() -> None:
    msg_id = new_id()
    record = channel_record(msg_id)
    record["message"]["content"] = [{"type": "text", "text": record["message"]["content"]}]
    assert channel_ids(record) == {msg_id}


def test_records_that_only_mention_an_id_do_not_count() -> None:
    msg_id = new_id()
    human = {
        "type": "user",
        "origin": {"kind": "human"},
        "message": {"content": f'<channel source="{CHANNEL_SOURCE}" msg_id="{msg_id}">'},
    }
    other_channel = channel_record(msg_id, source="plugin:telegram:telegram")
    assert channel_ids(human) == set()
    assert channel_ids(other_channel) == set()
    assert channel_ids("not a record") == set()


def _queue(operation: str, msg_id: str | None = None, reason: str | None = None) -> dict[str, Any]:
    record: dict[str, Any] = {"type": "queue-operation", "operation": operation}
    if msg_id:
        record["content"] = f'<channel source="{CHANNEL_SOURCE}" msg_id="{msg_id}">x</channel>'
    if reason:
        record["reason"] = reason
    return record


def _reader(tmp_path: Path, *records: object) -> Transcript:
    folder = tmp_path / "projects" / "p"
    folder.mkdir(parents=True, exist_ok=True)
    _append(folder / "S.jsonl", *records)
    return Transcript(tmp_path, "S")


def test_a_dequeue_confirms_the_oldest_queued_item(tmp_path: Path) -> None:
    first, second = new_id(), new_id()
    human = {"type": "queue-operation", "operation": "enqueue", "content": "a typed prompt"}
    reader = _reader(tmp_path, human, _queue("enqueue", first), _queue("enqueue", second))
    assert reader.new_channel_ids() == set()
    _append(tmp_path / "projects" / "p" / "S.jsonl", _queue("dequeue"), _queue("dequeue"))
    assert reader.new_channel_ids() == {first}


def test_an_enqueue_alone_confirms_nothing(tmp_path: Path) -> None:
    assert _reader(tmp_path, _queue("enqueue", new_id())).new_channel_ids() == set()


def test_delivering_removals_confirm_and_others_do_not(tmp_path: Path) -> None:
    absorbed, delivered, cancelled = new_id(), new_id(), new_id()
    reader = _reader(
        tmp_path,
        *(_queue("enqueue", i) for i in (absorbed, delivered, cancelled)),
        _queue("remove", absorbed, "absorbed_mid_turn"),
        _queue("remove", delivered, "delivered_to_agent"),
        _queue("remove", cancelled, "user_cancelled"),
    )
    assert reader.new_channel_ids() == {absorbed, delivered}


def test_pop_all_empties_the_queue_without_confirming(tmp_path: Path) -> None:
    msg_id = new_id()
    reader = _reader(tmp_path, _queue("enqueue", msg_id), _queue("popAll"), _queue("dequeue"))
    assert reader.new_channel_ids() == set()


def test_a_queued_command_attachment_confirms(tmp_path: Path) -> None:
    msg_id = new_id()
    tag = f'<channel source="{CHANNEL_SOURCE}" msg_id="{msg_id}">x</channel>'
    attachment = {"type": "attachment", "attachment": {"type": "queued_command", "prompt": tag}}
    assert _reader(tmp_path, attachment).new_channel_ids() == {msg_id}


def test_layout_known_needs_some_session_transcript(tmp_path: Path) -> None:
    reader = Transcript(tmp_path, "S")
    assert not reader.layout_known()
    folder = tmp_path / "projects" / "p"
    folder.mkdir(parents=True)
    _append(folder / "other-session.jsonl", {"type": "mode"})
    assert reader.layout_known()


def _append(path: Path, *records: object, newline: bool = True) -> None:
    with path.open("a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + ("\n" if newline else ""))


def test_transcript_is_found_by_session_id_and_read_incrementally(tmp_path: Path) -> None:
    folder = tmp_path / "projects" / "-Users-x-proj"
    folder.mkdir(parents=True)
    path = folder / "S1.jsonl"
    transcript = Transcript(tmp_path, "S1")
    assert transcript.path() is None

    first, second = new_id(), new_id()
    _append(path, {"type": "mode"}, channel_record(first))
    transcript = Transcript(tmp_path, "S1")
    assert transcript.path() == path
    assert transcript.new_channel_ids() == {first}
    assert transcript.new_channel_ids() == set()

    _append(path, channel_record(second), newline=False)
    assert transcript.new_channel_ids() == set()
    with path.open("a") as fh:
        fh.write("\n")
    assert transcript.new_channel_ids() == {second}
