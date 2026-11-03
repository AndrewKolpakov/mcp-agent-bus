"""Claude Code hook handlers: `agent-bus hook <event>`.

Hooks run in their own processes, independent of the MCP server, and reach
the bus through the same Store, so every write is serialized with the
server's. They are a safety net under the channel: SessionStart reports what
is waiting, Stop reminds the model of messages it has not handled, and
UserPromptSubmit records human activity, which revives a session the server
thought deaf.

The rest report what the session is doing, for the peers list (status.py):
a turn starts or ends, a permission prompt waits for a human, a tool runs
after one. They run with `async: true`, off the turn's path.

A hook never fails the session: any error is reported on stderr and the
hook exits 0, so the bus can only ever add context, never block a turn.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO

from agent_bus import config
from agent_bus.backend import HOOK_TIMEOUT_SECONDS, open_backend
from agent_bus.bus import Bus
from agent_bus.models import MessageEvent, now
from agent_bus.names import CHANNEL_PROMPT_PREFIX, PLUGIN_NAME, TOOL_PREFIX
from agent_bus.status import Activity

EVENTS = (
    "session-start",
    "user-prompt-submit",
    "stop",
    "turn-start",
    "turn-end",
    "permission-request",
    "post-tool-use",
)
ACTIVITY_EVENTS: dict[str, Activity] = {
    "turn-start": "working",
    "turn-end": "idle",
    "permission-request": "waiting_human",
    "post-tool-use": "working",
}
STATE_DIR_ENV = "AGENT_BUS_STATE_DIR"
"""Where `waiting-<session>` markers live; scripts/run.sh points it at the plugin data dir."""
PREVIEW_CHARS = 200
LISTED = 10


def _context(event_name: str, text: str) -> dict[str, Any]:
    return {"hookSpecificOutput": {"hookEventName": event_name, "additionalContext": text}}


def _listing(messages: list[MessageEvent]) -> str:
    lines = []
    for m in messages[:LISTED]:
        body = m.body.replace("\n", " ")
        if len(body) > PREVIEW_CHARS:
            body = body[:PREVIEW_CHARS] + "..."
        files = f" [{len(m.attachments)} attachment(s)]" if m.attachments else ""
        lines.append(f"- msg_id={m.id} from={m.sender} kind={m.kind}{files}: {body}")
    if len(messages) > LISTED:
        lines.append(f"- ... and {len(messages) - LISTED} more; call {TOOL_PREFIX}inbox")
    return "\n".join(lines)


def session_start(bus: Bus) -> dict[str, Any]:
    peers = bus.peers()
    roster = ", ".join(f"{p.name} ({'live' if p.live else 'offline'})" for p in peers) or "none yet"
    text = f"agent-bus: you are agent {bus.me}. Other agents: {roster}."
    unread = bus.inbox()
    if unread:
        text += (
            f"\n{len(unread)} unread message(s) from other agents are waiting. Handle each: "
            f"reply with {TOOL_PREFIX}send (reply_to=msg_id) or mark it handled with "
            f"{TOOL_PREFIX}ack.\n{_listing(unread)}"
        )
    return _context("SessionStart", text)


def user_prompt_submit(bus: Bus, payload: Mapping[str, Any]) -> dict[str, Any] | None:
    prompt = str(payload.get("prompt") or "")
    # Channel events from this bus also fire UserPromptSubmit (stage 0, Q6).
    # They are agent turns, so they must not reset the ping-pong counter; any
    # other prompt, including events from other channels, is a human's.
    if prompt.lstrip().startswith(CHANNEL_PROMPT_PREFIX):
        return None
    bus.record_human_turn(str(payload.get("session_id") or "unknown"))
    return None


def stop(bus: Bus, payload: Mapping[str, Any]) -> dict[str, Any] | None:
    if payload.get("stop_hook_active"):
        return None
    unread = bus.inbox()
    if not unread:
        return None
    text = (
        f"agent-bus: {len(unread)} message(s) from other agents are still unhandled. "
        f"For each, either reply with {TOOL_PREFIX}send (reply_to=msg_id) or call "
        f"{TOOL_PREFIX}ack if it needs no answer. Never answer kind=await_human; just ack it.\n"
        f"{_listing(unread)}"
    )
    return _context("Stop", text)


def _marker(state_dir: Path, session: str) -> Path:
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in session)[:128]
    return state_dir / f"waiting-{safe}"


def activity(
    bus: Bus, event: str, payload: Mapping[str, Any], state_dir: Path, at: datetime
) -> None:
    """Report the session's activity (status.py) as of `at`, when the hook started.

    These hooks run in the background, so their reports may land out of
    order; the time orders them. PostToolUse fires after every tool call,
    and only the first one after a permission prompt has news: the
    `waiting-<session>` marker says which, and scripts/run.sh skips
    starting Python at all while no marker exists.
    """
    session = str(payload.get("session_id") or "unknown")
    marker = _marker(state_dir, session)
    state = ACTIVITY_EVENTS[event]
    if event == "post-tool-use" and not marker.exists():
        return
    if state == "waiting_human":
        state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        marker.touch(mode=0o600)
    else:
        marker.unlink(missing_ok=True)
    bus.report_activity(session, state, at)


def run(
    event: str,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    # Background hooks of one session start in order but may run at
    # different speeds; their reports are ordered by when they started.
    started = now()
    source: TextIO = sys.stdin if stdin is None else stdin
    sink: TextIO = sys.stdout if stdout is None else stdout
    try:
        cfg = config.load(env)
        if not cfg.on_bus:
            return 0
        raw = source.read()
        payload: Mapping[str, Any] = json.loads(raw) if raw.strip() else {}
        if cfg.agent is None:
            if event == "session-start":
                text = (
                    f"agent-bus: this session opted in to the bus but agent_name is not set. "
                    f"Tell your user to run `claude plugin configure {PLUGIN_NAME}`."
                )
                json.dump(_context("SessionStart", text), sink)
            return 0
        backend = open_backend(cfg, timeout=HOOK_TIMEOUT_SECONDS)
        bus = Bus(
            backend.store,
            cfg.agent,
            registry=backend.registry,
            rate_limit=cfg.rate_limit,
            rate_window=cfg.rate_window,
        )
        if event == "session-start":
            output: dict[str, Any] | None = session_start(bus)
        elif event == "user-prompt-submit":
            output = user_prompt_submit(bus, payload)
        elif event == "stop":
            output = stop(bus, payload)
        elif event in ACTIVITY_EVENTS:
            environ = env if env is not None else os.environ
            state_dir = Path(environ.get(STATE_DIR_ENV) or cfg.bus_dir)
            activity(bus, event, payload, state_dir, started)
            output = None
        else:
            raise ValueError(f"unknown hook event: {event}")
        if output is not None:
            json.dump(output, sink)
    except Exception as exc:
        print(f"agent-bus hook {event}: {exc}", file=sys.stderr)
    return 0
