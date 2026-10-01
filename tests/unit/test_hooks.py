from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest

from agent_bus import hooks
from agent_bus.bus import Bus
from agent_bus.models import HumanTurnEvent, SessionReadyEvent
from agent_bus.names import CHANNEL_PROMPT_PREFIX, TOOL_PREFIX
from agent_bus.store import Store


def _env(bus_dir: Path, **extra: str) -> dict[str, str]:
    return {
        "AGENT_BUS_SESSION": "1",
        "AGENT_BUS_NAME": "bob",
        "AGENT_BUS_DIR": str(bus_dir),
        **extra,
    }


def _run(event: str, env: dict[str, str], payload: dict[str, Any] | None = None) -> str:
    out = io.StringIO()
    code = hooks.run(event, stdin=io.StringIO(json.dumps(payload or {})), stdout=out, env=env)
    assert code == 0
    return out.getvalue()


def _context(output: str) -> str:
    text: str = json.loads(output)["hookSpecificOutput"]["additionalContext"]
    return text


@pytest.fixture
def bus_dir(tmp_path: Path) -> Path:
    return tmp_path / "bus"


def test_sessions_off_the_bus_get_nothing(bus_dir: Path) -> None:
    Bus(Store(bus_dir), "alice", 6).send("bob", "hi")
    env = {"AGENT_BUS_NAME": "bob", "AGENT_BUS_DIR": str(bus_dir)}
    for event in hooks.EVENTS:
        assert _run(event, env, {"prompt": "x", "session_id": "s"}) == ""
    assert not [e for e in Store(bus_dir).read_all().events if isinstance(e, HumanTurnEvent)]


def test_session_start_names_the_agent_and_lists_unread(bus_dir: Path) -> None:
    msg = Bus(Store(bus_dir), "alice", 6).send("bob", "please review\nthe diff", kind="task")
    output = json.loads(_run("session-start", _env(bus_dir)))
    assert output["hookSpecificOutput"]["hookEventName"] == "SessionStart"
    text = output["hookSpecificOutput"]["additionalContext"]
    assert "you are agent bob" in text
    assert "alice (offline)" in text
    assert f"msg_id={msg.id} from=alice kind=task: please review the diff" in text
    assert f"{TOOL_PREFIX}ack" in text


def test_session_start_marks_the_claude_session_ready(bus_dir: Path) -> None:
    _run("session-start", _env(bus_dir), {"session_id": "claude-7"})
    ready = [e for e in Store(bus_dir).read_all().events if isinstance(e, SessionReadyEvent)]
    assert [(r.agent, r.session) for r in ready] == [("bob", "claude-7")]


def test_session_start_without_an_agent_name_says_what_to_configure(bus_dir: Path) -> None:
    env = {"AGENT_BUS_SESSION": "1", "AGENT_BUS_DIR": str(bus_dir)}
    assert "claude plugin configure agent-bus" in _context(_run("session-start", env))
    assert _run("stop", env) == ""


def test_a_human_prompt_records_a_human_turn(bus_dir: Path) -> None:
    _run("user-prompt-submit", _env(bus_dir), {"prompt": "hello", "session_id": "claude-1"})
    turns = [e for e in Store(bus_dir).read_all().events if isinstance(e, HumanTurnEvent)]
    assert [(t.agent, t.session) for t in turns] == [("bob", "claude-1")]


@pytest.mark.parametrize(
    ("prompt", "counts_as_human"),
    [
        (f'{CHANNEL_PROMPT_PREFIX} from="alice" msg_id="X">\nping\n</channel>', False),
        (f'  {CHANNEL_PROMPT_PREFIX} from="alice">x</channel>', False),
        ('<channel source="plugin:telegram:telegram" chat_id="1">hi</channel>', True),
        ("please ask alice about <channel source=", True),
    ],
)
def test_only_this_bus_channel_events_are_not_human_turns(
    bus_dir: Path, prompt: str, counts_as_human: bool
) -> None:
    _run("user-prompt-submit", _env(bus_dir), {"prompt": prompt, "session_id": "s"})
    turns = [e for e in Store(bus_dir).read_all().events if isinstance(e, HumanTurnEvent)]
    assert bool(turns) is counts_as_human


def test_stop_reminds_about_unhandled_messages_once_per_chain(bus_dir: Path) -> None:
    msg = Bus(Store(bus_dir), "alice", 6).send("bob", "fyi")
    output = json.loads(_run("stop", _env(bus_dir), {"stop_hook_active": False}))
    assert output["hookSpecificOutput"]["hookEventName"] == "Stop"
    assert msg.id in output["hookSpecificOutput"]["additionalContext"]
    assert _run("stop", _env(bus_dir), {"stop_hook_active": True}) == ""
    Bus(Store(bus_dir), "bob", 6).ack(msg.id)
    assert _run("stop", _env(bus_dir), {"stop_hook_active": False}) == ""


def test_long_listings_are_truncated(bus_dir: Path) -> None:
    alice = Bus(Store(bus_dir), "alice", 50)
    for i in range(hooks.LISTED + 3):
        alice.send("bob", f"m{i} " + "x" * 500)
    text = _context(_run("stop", _env(bus_dir)))
    assert text.count("- msg_id=") == hooks.LISTED
    assert "and 3 more" in text
    assert "x" * (hooks.PREVIEW_CHARS + 1) not in text


def test_a_broken_hook_never_fails_the_turn(
    bus_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env = _env(bus_dir, AGENT_BUS_NAME="Bad Name")
    out = io.StringIO()
    assert hooks.run("stop", stdin=io.StringIO("{}"), stdout=out, env=env) == 0
    assert out.getvalue() == ""
    assert "invalid agent name" in capsys.readouterr().err
