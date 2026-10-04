"""`delivered` follows the session transcript, not the push (stage 5).

Each server here runs as a Claude Code session with its own session id and
a transcript file under a temporary CLAUDE_CONFIG_DIR. The test plays the
part of Claude Code: it writes the channel record into the transcript when
the session "receives" an event, or leaves it out to make the session deaf.
"""

from __future__ import annotations

import json
import signal
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from agent_bus.bus import Bus
from agent_bus.models import DeliveredEvent
from agent_bus.names import CHANNEL_SOURCE
from agent_bus.registry import Registry
from agent_bus.store import Store

from .mcp_client import ServerProcess

CHANNEL = "notifications/claude/channel"


class Session:
    def __init__(self, server: ServerProcess, transcript: Path) -> None:
        self.server = server
        self.transcript = transcript

    def receive(self, timeout: float = 20.0) -> str:
        """Take the next channel event and record it as Claude Code would."""
        event = self.server.channel_events(timeout)
        msg_id: str = event["params"]["meta"]["msg_id"]
        self.record(msg_id)
        return msg_id

    def record(self, msg_id: str) -> None:
        record = {
            "type": "user",
            "origin": {"kind": "channel", "server": CHANNEL_SOURCE},
            "message": {
                "role": "user",
                "content": f'<channel source="{CHANNEL_SOURCE}" msg_id="{msg_id}">x</channel>',
            },
        }
        with self.transcript.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")

    def queue(self, operation: str, msg_id: str | None = None, reason: str | None = None) -> None:
        """Write a queue-operation record the way Claude Code 2.1.286 does."""
        record: dict[str, object] = {"type": "queue-operation", "operation": operation}
        if msg_id is not None:
            record["content"] = f'<channel source="{CHANNEL_SOURCE}" msg_id="{msg_id}">x</channel>'
        if reason is not None:
            record["reason"] = reason
        with self.transcript.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")

    def no_push(self, seconds: float) -> bool:
        return self.server.collect(lambda m: m.get("method") == CHANNEL, seconds) == []


Open = Callable[..., Session]


@pytest.fixture
def bus_dir(tmp_path: Path) -> Path:
    return tmp_path / "bus"


@pytest.fixture
def open_session(tmp_path: Path, bus_dir: Path) -> Iterator[Open]:
    servers: list[ServerProcess] = []
    config_dir = tmp_path / "claude-config"

    def _open(session_id: str, *, transcript: bool = True, confirm: str = "30") -> Session:
        folder = config_dir / "projects" / "-tmp-project"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{session_id}.jsonl"
        if transcript:
            path.write_text('{"type": "mode"}\n')
        env = {
            "AGENT_BUS_SESSION": "1",
            "AGENT_BUS_NAME": "bob",
            "AGENT_BUS_SETTLE_SECONDS": "0",
            "AGENT_BUS_HEARTBEAT_SECONDS": "0.3",
            "AGENT_BUS_CONFIRM_SECONDS": confirm,
            "CLAUDE_CONFIG_DIR": str(config_dir),
            "CLAUDE_CODE_SESSION_ID": session_id,
        }
        server = ServerProcess(bus_dir, env)
        servers.append(server)
        server.handshake()
        server.request("tools/list")
        return Session(server, path)

    yield _open
    for server in servers:
        server.stop()
        assert server.bad_lines == [], f"non-JSON on stdout: {server.bad_lines}"


def _alice(bus_dir: Path) -> Bus:
    return Bus(Store(bus_dir), "alice", rate_limit=50)


def _delivered(bus_dir: Path) -> list[str]:
    return [e.id for e in Store(bus_dir).read_all().events if isinstance(e, DeliveredEvent)]


def _wait(predicate: Callable[[], bool], timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return False


def test_delivered_is_written_only_once_the_transcript_shows_the_event(
    open_session: Open, bus_dir: Path
) -> None:
    session = open_session("S1")
    msg = _alice(bus_dir).send("bob", "hello")
    event = session.server.channel_events()
    assert event["params"]["meta"]["msg_id"] == msg.id
    time.sleep(1.0)
    assert _delivered(bus_dir) == []
    session.record(msg.id)
    assert _wait(lambda: _delivered(bus_dir) == [msg.id])


def test_a_deaf_session_releases_the_channel_and_another_replays(
    open_session: Open, bus_dir: Path
) -> None:
    deaf = open_session("S-deaf", confirm="1")
    msg = _alice(bus_dir).send("bob", "nobody hears this one")
    deaf.server.channel_events()  # pushed, but never reaches the transcript
    registry = Registry(Store(bus_dir))
    assert _wait(lambda: registry.read("bob") is None)
    assert _delivered(bus_dir) == []
    assert "is deaf" in (bus_dir / "server.log").read_text()

    alive = open_session("S-alive")
    assert alive.receive() == msg.id
    assert _wait(lambda: _delivered(bus_dir) == [msg.id])
    # The deaf session does not grab the channel back while it stays deaf.
    assert deaf.no_push(1.0)


def test_a_late_confirmation_settles_the_message_and_restores_the_session(
    open_session: Open, bus_dir: Path
) -> None:
    session = open_session("S1", confirm="1")
    first = _alice(bus_dir).send("bob", "seen only after a long turn")
    session.server.channel_events()
    registry = Registry(Store(bus_dir))
    assert _wait(lambda: registry.read("bob") is None)
    session.record(first.id)
    assert _wait(lambda: _delivered(bus_dir) == [first.id])
    assert _wait(lambda: registry.read("bob") is not None)
    second = _alice(bus_dir).send("bob", "and the channel works again")
    assert session.receive() == second.id


def _go_deaf(session: Session, bus_dir: Path) -> str:
    msg = _alice(bus_dir).send("bob", "dropped while the dialog was open")
    session.server.channel_events()  # pushed, never reaches the transcript
    registry = Registry(Store(bus_dir))
    assert _wait(lambda: registry.read("bob") is None)
    return msg.id


def _replayed(session: Session) -> dict[str, str]:
    event = session.server.channel_events()
    session.record(event["params"]["meta"]["msg_id"])
    meta: dict[str, str] = event["params"]["meta"]
    return meta


def test_a_tool_call_revives_a_deaf_session_and_replays_what_it_lost(
    open_session: Open, bus_dir: Path
) -> None:
    session = open_session("S1", confirm="1")
    lost = _go_deaf(session, bus_dir)
    session.server.call("inbox")
    meta = _replayed(session)
    assert (meta["msg_id"], meta["redelivery"]) == (lost, "true")
    assert _wait(lambda: _delivered(bus_dir) == [lost])


def test_a_human_turn_of_this_session_revives_it(open_session: Open, bus_dir: Path) -> None:
    session = open_session("S1", confirm="1")
    lost = _go_deaf(session, bus_dir)
    Bus(Store(bus_dir), "bob").record_human_turn("some-other-session")
    assert session.no_push(1.5)
    Bus(Store(bus_dir), "bob").record_human_turn("S1")
    meta = _replayed(session)
    assert (meta["msg_id"], meta["redelivery"]) == (lost, "true")


def test_with_no_transcripts_at_all_an_overdue_push_counts_as_delivered(
    open_session: Open, bus_dir: Path
) -> None:
    session = open_session("S-no-file", transcript=False, confirm="1")
    msg = _alice(bus_dir).send("bob", "unknown layout")
    session.server.channel_events()
    time.sleep(0.5)
    assert _delivered(bus_dir) == []
    assert _wait(lambda: _delivered(bus_dir) == [msg.id])
    assert Registry(Store(bus_dir)).read("bob") is not None


def test_a_fresh_session_whose_transcript_appears_with_the_event_is_confirmed(
    open_session: Open, bus_dir: Path
) -> None:
    session = open_session("S-fresh", transcript=False)
    msg = _alice(bus_dir).send("bob", "first turn of a fresh session")
    session.server.channel_events()
    session.queue("enqueue", msg.id)
    session.queue("dequeue")
    assert _wait(lambda: _delivered(bus_dir) == [msg.id])


def test_an_enqueue_without_a_dequeue_does_not_confirm(open_session: Open, bus_dir: Path) -> None:
    # What a foreground process whose conversation moved to the background writes.
    session = open_session("S-forked-away", confirm="1")
    msg = _alice(bus_dir).send("bob", "queued in a process nobody talks to")
    session.server.channel_events()
    session.queue("enqueue", msg.id)
    assert _wait(lambda: Registry(Store(bus_dir)).read("bob") is None)
    assert _delivered(bus_dir) == []


def test_a_message_absorbed_mid_turn_is_confirmed(open_session: Open, bus_dir: Path) -> None:
    session = open_session("S-busy")
    msg = _alice(bus_dir).send("bob", "arrives while the model is busy")
    session.server.channel_events()
    session.queue("enqueue", msg.id)
    session.queue("remove", msg.id, reason="absorbed_mid_turn")
    assert _wait(lambda: _delivered(bus_dir) == [msg.id])


def test_a_waiting_session_takes_over_when_the_holder_ends(
    open_session: Open, bus_dir: Path
) -> None:
    holder = open_session("S-holder")
    waiting = open_session("S-waiting")
    registry = Registry(Store(bus_dir))
    record = registry.read("bob")
    assert record is not None
    assert record.pid == holder.server.proc.pid
    holder.server.proc.send_signal(signal.SIGTERM)
    assert holder.server.proc.wait(timeout=10) == 0
    assert _wait(
        lambda: (r := registry.read("bob")) is not None and r.pid == waiting.server.proc.pid
    )
    msg = _alice(bus_dir).send("bob", "for whoever holds the channel now")
    assert waiting.receive() == msg.id
