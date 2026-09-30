"""`agent-bus serve` driven over stdio the way Claude Code drives it."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from agent_bus.bus import Bus
from agent_bus.models import DeliveredEvent
from agent_bus.registry import Registry
from agent_bus.store import Store

from .mcp_client import DISCOVER_PROBE, ServerProcess, tool_json

ON_BUS = {"AGENT_BUS_SESSION": "1", "AGENT_BUS_NAME": "bob", "AGENT_BUS_MAX_TURNS": "4"}

Spawn = Callable[..., ServerProcess]


@pytest.fixture
def bus_dir(tmp_path: Path) -> Path:
    return tmp_path / "bus"


@pytest.fixture
def spawn(bus_dir: Path) -> Iterator[Spawn]:
    started: list[ServerProcess] = []

    def _spawn(env: dict[str, str] | None = None, *, handshake: bool = True) -> ServerProcess:
        server = ServerProcess(bus_dir, ON_BUS if env is None else env)
        started.append(server)
        if handshake:
            server.handshake()
        return server

    yield _spawn
    for server in started:
        server.stop()
        assert server.bad_lines == [], f"non-JSON on stdout: {server.bad_lines}"


def _alice(bus_dir: Path) -> Bus:
    return Bus(Store(bus_dir), "alice", max_agent_turns=4)


def test_discover_probe_is_refused_and_initialize_declares_the_channel(spawn: Spawn) -> None:
    server = spawn(handshake=False)
    server.send(DISCOVER_PROBE)
    probe = server.wait_for(lambda m: m.get("id") == DISCOVER_PROBE["id"])
    assert "error" in probe
    init = server.handshake()["result"]
    assert init["protocolVersion"] == "2025-11-25"
    assert init["capabilities"]["experimental"] == {"claude/channel": {}}
    assert 'You are agent "bob"' in init["instructions"]
    tools = server.request("tools/list")["result"]["tools"]
    assert sorted(t["name"] for t in tools) == ["ack", "inbox", "peers", "send"]


def test_a_new_message_is_pushed_as_a_channel_event(spawn: Spawn, bus_dir: Path) -> None:
    server = spawn()
    time.sleep(0.5)
    sent = _alice(bus_dir).send("bob", "ping", kind="task")
    event = server.channel_events()
    assert event["params"]["content"] == "ping"
    assert event["params"]["meta"] == {
        "from": "alice",
        "kind": "task",
        "msg_id": sent.id,
        "thread": sent.thread,
    }
    delivered = [e for e in Store(bus_dir).read_all().events if isinstance(e, DeliveredEvent)]
    assert [d.id for d in delivered] == [sent.id]


def test_messages_sent_while_closed_are_replayed_once(spawn: Spawn, bus_dir: Path) -> None:
    queued = _alice(bus_dir).send("bob", "while you were away")
    first = spawn()
    event = first.channel_events()
    assert event["params"]["meta"]["msg_id"] == queued.id
    assert event["params"]["meta"]["redelivery"] == "true"
    # The watcher starts at the replay's own offset, so it must not push it again.
    assert first.collect(lambda m: m.get("method") == "notifications/claude/channel", 1.5) == []
    first.close_stdin()
    first.proc.wait(timeout=10)

    second = spawn()
    assert second.collect(lambda m: m.get("method") == "notifications/claude/channel", 2.0) == []


def test_tools_send_inbox_ack_and_peers(spawn: Spawn, bus_dir: Path) -> None:
    server = spawn()
    question = _alice(bus_dir).send("bob", "question")
    server.channel_events()

    inbox = tool_json(server.call("inbox"))
    assert [m["id"] for m in inbox] == [question.id]
    assert inbox[0]["from"] == "alice"

    reply = tool_json(
        server.call("send", {"to": "alice", "body": "answer", "reply_to": question.id})
    )
    assert reply["thread"] == question.thread
    assert tool_json(server.call("inbox")) == []
    assert [m.body for m in _alice(bus_dir).inbox()] == ["answer"]

    other = _alice(bus_dir).send("bob", "fyi")
    server.channel_events()
    assert tool_json(server.call("ack", {"ids": [other.id]})) == {
        "acked": [other.id],
        "already_acked": [],
    }
    peers = tool_json(server.call("peers"))
    # alice has not acked bob's answer yet.
    assert peers == [{"name": "alice", "live": False, "unread_by_them": 1}]


def test_tool_errors_come_back_as_error_results(spawn: Spawn, bus_dir: Path) -> None:
    server = spawn()
    alice = _alice(bus_dir)
    for i in range(2):
        alice.send("bob", f"a{i}")
        tool_json(server.call("send", {"to": "alice", "body": f"b{i}"}))
    refused = server.call("send", {"to": "alice", "body": "one more"})
    assert refused["isError"] is True
    assert "await_human" in refused["content"][0]["text"]
    handoff = server.call("send", {"to": "alice", "body": "over to humans", "kind": "await_human"})
    assert not handoff.get("isError")
    bad = server.call("send", {"to": "bob", "body": "self"})
    assert bad["isError"] is True


def test_a_session_off_the_bus_lists_no_tools_and_pushes_nothing(
    spawn: Spawn, bus_dir: Path
) -> None:
    server = spawn({"AGENT_BUS_NAME": "bob"})
    init_tools = server.request("tools/list")["result"]["tools"]
    assert init_tools == []
    _alice(bus_dir).send("bob", "anyone?")
    assert server.collect(lambda m: m.get("method") == "notifications/claude/channel", 1.5) == []
    assert Registry(Store(bus_dir)).live() == {}


def test_a_second_session_of_the_same_agent_gets_no_pushes(spawn: Spawn, bus_dir: Path) -> None:
    first = spawn()
    second = spawn(handshake=False)
    init = second.handshake()["result"]
    assert "holds the channel" in init["instructions"]
    _alice(bus_dir).send("bob", "only once")
    first.channel_events()
    assert second.collect(lambda m: m.get("method") == "notifications/claude/channel", 1.5) == []


def test_closing_stdin_stops_the_server_and_releases_the_channel(
    spawn: Spawn, bus_dir: Path
) -> None:
    server = spawn()
    registry = Registry(Store(bus_dir))
    assert set(registry.live()) == {"bob"}
    server.close_stdin()
    assert server.proc.wait(timeout=10) == 0
    assert registry.read("bob") is None


def test_server_log_records_env_key_names_not_values(spawn: Spawn, bus_dir: Path) -> None:
    server = spawn({**ON_BUS, "CLAUDE_PLUGIN_OPTION_SECRETISH": "do-not-log-me"})
    server.close_stdin()
    server.proc.wait(timeout=10)
    text = (bus_dir / "server.log").read_text()
    assert "CLAUDE_PLUGIN_OPTION_SECRETISH" in text
    assert "do-not-log-me" not in text
