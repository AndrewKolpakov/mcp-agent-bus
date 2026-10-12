"""`agent-bus serve` driven over stdio the way Claude Code drives it."""

from __future__ import annotations

import json
import os
import signal
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from agent_bus.bus import Bus
from agent_bus.models import DeliveredEvent
from agent_bus.registry import Registry
from agent_bus.store import Store

from .mcp_client import DISCOVER_PROBE, ServerProcess, tool_json

ON_BUS = {
    "AGENT_BUS_SESSION": "1",
    "AGENT_BUS_NAME": "bob",
    "AGENT_BUS_RATE_LIMIT": "4",
    "AGENT_BUS_SETTLE_SECONDS": "0",
}

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
            server.connect()
        return server

    yield _spawn
    for server in started:
        server.stop()
        assert server.bad_lines == [], f"non-JSON on stdout: {server.bad_lines}"


def _alice(bus_dir: Path) -> Bus:
    return Bus(Store(bus_dir), "alice", rate_limit=4)


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


def test_opting_in_without_an_agent_name_says_what_to_configure(spawn: Spawn) -> None:
    unnamed = spawn({"AGENT_BUS_SESSION": "1"}, handshake=False)
    init = unnamed.handshake()["result"]
    assert "claude plugin configure agent-bus" in init["instructions"]
    assert unnamed.request("tools/list")["result"]["tools"] == []


def test_a_session_that_loses_its_claim_stops_pushing(spawn: Spawn, bus_dir: Path) -> None:
    server = spawn({**ON_BUS, "AGENT_BUS_HEARTBEAT_SECONDS": "0.3"})
    registry = Registry(Store(bus_dir))
    record = registry.read("bob")
    assert record is not None
    # Another live session takes over, as after this one slept past STALE_AFTER.
    registry._write(record.model_copy(update={"session": "someone-else", "pid": os.getpid()}))
    time.sleep(1.5)
    _alice(bus_dir).send("bob", "who gets this?")
    assert server.collect(lambda m: m.get("method") == "notifications/claude/channel", 1.5) == []


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


def _no_push(server: ServerProcess, seconds: float) -> bool:
    return (
        server.collect(lambda m: m.get("method") == "notifications/claude/channel", seconds) == []
    )


def test_nothing_is_pushed_before_claude_code_lists_the_tools(spawn: Spawn, bus_dir: Path) -> None:
    queued = _alice(bus_dir).send("bob", "waiting since before the session started")
    server = spawn(handshake=False)
    server.handshake()
    assert _no_push(server, 1.5)
    assert not [e for e in Store(bus_dir).read_all().events if isinstance(e, DeliveredEvent)]
    server.request("tools/list")
    event = server.channel_events()
    assert event["params"]["meta"]["msg_id"] == queued.id
    assert event["params"]["meta"]["redelivery"] == "true"


def test_pushes_wait_the_settle_delay_after_the_tools_list(spawn: Spawn, bus_dir: Path) -> None:
    queued = _alice(bus_dir).send("bob", "replayed after the settle delay")
    server = spawn({**ON_BUS, "AGENT_BUS_SETTLE_SECONDS": "2"})
    assert _no_push(server, 1.0)
    assert server.channel_events(timeout=10)["params"]["meta"]["msg_id"] == queued.id


def test_a_background_session_never_takes_the_channel(spawn: Spawn, bus_dir: Path) -> None:
    background = spawn({**ON_BUS, "CLAUDE_JOB_DIR": "/tmp/job"}, handshake=False)
    init = background.handshake()["result"]
    assert "background session" in init["instructions"]
    tools = background.request("tools/list")["result"]["tools"]
    assert sorted(t["name"] for t in tools) == ["ack", "inbox", "peers", "send"]
    assert Registry(Store(bus_dir)).live() == {}
    msg = _alice(bus_dir).send("bob", "for the interactive session")
    assert (
        background.collect(lambda m: m.get("method") == "notifications/claude/channel", 1.5) == []
    )
    assert not [e for e in Store(bus_dir).read_all().events if isinstance(e, DeliveredEvent)]
    interactive = spawn()
    assert interactive.channel_events()["params"]["meta"]["msg_id"] == msg.id


def test_a_session_without_channels_reads_inbox_and_never_counts_delivered(
    spawn: Spawn, bus_dir: Path
) -> None:
    """Codex ignores a push; counting it delivered would hide the message for good."""
    codex = spawn({**ON_BUS, "AGENT_BUS_CHANNEL": "0"}, handshake=False)
    instructions = codex.handshake()["result"]["instructions"]
    assert "Call inbox at the start of every turn" in instructions
    assert "<channel" not in instructions
    assert Registry(Store(bus_dir)).live() == {}
    msg = _alice(bus_dir).send("bob", "for codex")
    assert codex.collect(lambda m: m.get("method") == "notifications/claude/channel", 1.5) == []
    assert not [e for e in Store(bus_dir).read_all().events if isinstance(e, DeliveredEvent)]
    inbox = tool_json(codex.call("inbox"))
    assert [m["id"] for m in inbox] == [msg.id]
    tool_json(codex.call("ack", {"ids": [msg.id]}))
    assert Bus(Store(bus_dir), "alice").peers()[0].unread_by_them == 0


def test_inbox_waits_for_a_message_that_arrives_during_the_call(
    spawn: Spawn, bus_dir: Path
) -> None:
    codex = spawn({**ON_BUS, "AGENT_BUS_CHANNEL": "0"})
    started = time.monotonic()
    assert tool_json(codex.call("inbox", {"wait_seconds": 1})) == []
    assert time.monotonic() - started >= 1

    request_id = 900
    codex.send(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": "inbox", "arguments": {"wait_seconds": 30}},
        }
    )
    time.sleep(1)
    msg = _alice(bus_dir).send("bob", "late answer")
    started = time.monotonic()
    result = codex.wait_for(lambda m: m.get("id") == request_id)["result"]
    assert time.monotonic() - started < 5
    assert [m["id"] for m in tool_json(result)] == [msg.id]


def test_codex_gets_each_message_queued_into_its_thread(
    spawn: Spawn, bus_dir: Path, tmp_path: Path
) -> None:
    calls = tmp_path / "codex-calls.jsonl"
    fake = tmp_path / "codex"
    fake.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        f"open({str(calls)!r}, 'a').write(json.dumps(sys.argv[1:]) + '\\n')\n"
    )
    fake.chmod(0o755)
    codex = spawn({**ON_BUS, "AGENT_BUS_CHANNEL": "codex", "AGENT_BUS_CODEX_BIN": str(fake)})
    assert set(Registry(Store(bus_dir)).live()) == {"bob"}
    early = _alice(bus_dir).send("bob", "before codex said where it is")
    time.sleep(1)
    assert not calls.exists(), "queued before the thread was known"

    meta = {"x-codex-turn-metadata": {"thread_id": "thread-1"}, "threadId": "thread-1"}
    codex.request("tools/call", {"name": "peers", "arguments": {}, "_meta": meta})
    late = _alice(bus_dir).send("bob", "after")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and len(_lines(calls)) < 2:
        time.sleep(0.2)
    queued = [json.loads(line) for line in _lines(calls)]
    assert [q[:4] for q in queued] == [["queue", "--thread", "thread-1", "--message"]] * 2
    assert f'msg_id="{early.id}"' in queued[0][4]
    assert 'redelivery="true"' in queued[0][4]
    assert queued[1][4].endswith("\nafter\n</agent-bus-message>")
    delivered = {e.id for e in Store(bus_dir).read_all().events if isinstance(e, DeliveredEvent)}
    assert delivered == {early.id, late.id}


def _lines(path: Path) -> list[str]:
    return path.read_text().splitlines() if path.exists() else []


def test_sigterm_releases_the_channel_and_logs_stop(spawn: Spawn, bus_dir: Path) -> None:
    server = spawn()
    registry = Registry(Store(bus_dir))
    assert set(registry.live()) == {"bob"}
    server.proc.send_signal(signal.SIGTERM)
    assert server.proc.wait(timeout=10) == 0
    assert registry.read("bob") is None
    assert "signal=SIGTERM" in (bus_dir / "server.log").read_text()


def test_the_discover_probe_does_not_flood_server_log(spawn: Spawn, bus_dir: Path) -> None:
    server = spawn()
    server.close_stdin()
    server.proc.wait(timeout=10)
    text = (bus_dir / "server.log").read_text()
    assert "Failed to validate request" not in text
    assert "validation error" not in text
