"""Command-line entry point: `agent-bus <command>`.

The MCP server and every hook run through this one entry point, so the
plugin needs a single executable on the bus.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from collections.abc import Sequence

from agent_bus import __version__, admin, config, hooks
from agent_bus.backend import open_backend
from agent_bus.bus import Bus, BusError
from agent_bus.models import KINDS, Event, MessageEvent
from agent_bus.store import EventStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-bus",
        description="Message bus between Claude Code sessions on different accounts.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--bus-dir", help="bus directory (default: $AGENT_BUS_DIR or ~/.agent-bus)")
    parser.add_argument(
        "--as",
        dest="agent",
        help="act as this agent (default: $AGENT_BUS_AS, then $AGENT_BUS_NAME)",
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>")

    send = sub.add_parser("send", help="send a message to another agent")
    send.add_argument("--to", required=True, help="recipient agent name")
    send.add_argument("--kind", choices=KINDS, default="text")
    send.add_argument("--reply-to", help="id of the message this answers")
    send.add_argument("body", help="message text, or '-' to read it from stdin")

    inbox = sub.add_parser("inbox", help="list messages addressed to you")
    inbox.add_argument("--all", action="store_true", help="include acknowledged messages")
    inbox.add_argument("--json", action="store_true", help="one JSON event per line")

    ack = sub.add_parser("ack", help="mark messages as handled")
    ack.add_argument("ids", nargs="+", metavar="id")

    peers = sub.add_parser("peers", help="list other agents on the bus")
    peers.add_argument("--json", action="store_true")

    sub.add_parser("serve", help="run the MCP stdio server (Claude Code starts this)")

    hook = sub.add_parser("hook", help="run a Claude Code hook (Claude Code starts this)")
    hook.add_argument("event", choices=hooks.EVENTS)

    sub.add_parser(
        "hub-token",
        help="store this agent's hub token, read from stdin, in a file only you can read",
    )
    sub.add_parser("sync", help="bring the local mirror of a hub up to date")

    codex = sub.add_parser(
        "codex",
        help="start a Codex session on the bus as this agent",
        description="Start Codex with this agent's bus server, for this session only. "
        "Arguments after -- go to codex.",
    )
    codex.add_argument("codex_args", nargs=argparse.REMAINDER, help="arguments for codex")

    admin.add_parser(sub)

    tail = sub.add_parser("tail", help="print the event log")
    tail.add_argument("-f", "--follow", action="store_true", help="keep printing new events")
    tail.add_argument("--json", action="store_true", help="raw log lines")
    return parser


def format_event(event: Event) -> str:
    stamp = event.ts.astimezone().strftime("%H:%M:%S")
    if isinstance(event, MessageEvent):
        reply = f" re {event.reply_to}" if event.reply_to else ""
        body = event.body.replace("\n", "\n    ")
        return f"{stamp} {event.sender} -> {event.to} [{event.kind}] {event.id}{reply}\n    {body}"
    if event.type == "human_turn":
        return f"{stamp} human turn for {event.agent}"
    if event.type == "session_ready":
        return f"{stamp} session ready for {event.agent}"
    if event.type == "delivered":
        return f"{stamp} delivered {event.id} to {event.to}"
    return f"{stamp} ack {event.id} by {event.by}"


def _dump(event: Event) -> str:
    return event.model_dump_json(by_alias=True, exclude_none=True)


def _tail(store: EventStore, *, follow: bool, raw: bool) -> int:
    result = store.read_all()
    offset = result.offset
    for event in result.events:
        print(_dump(event) if raw else format_event(event), flush=True)
    while follow:
        time.sleep(0.3)
        result = store.read_from(offset)
        offset = result.offset
        for event in result.events:
            print(_dump(event) if raw else format_event(event), flush=True)
    return 0


def _hub_token(cfg: config.Config) -> int:
    if cfg.hub is None:
        raise BusError("no hub configured: set AGENT_BUS_HUB or the plugin's hub_url")
    from agent_bus.remote import TOKEN_NAME, hub_home, write_token

    token = sys.stdin.read().strip()
    if not token:
        raise BusError("no token on stdin")
    path = hub_home(cfg.bus_dir, cfg.hub, cfg.require_agent()) / TOKEN_NAME
    write_token(path, token)
    print(f"saved to {path}")
    return 0


CODEX_SERVER = "agent-bus"
CODEX_OPENING = (
    'You are on agent-bus as agent "{me}". Call its peers tool once, then tell me in one '
    "line who else is on the bus."
)


def codex_argv(cfg: config.Config, codex_bin: str, extra: Sequence[str]) -> list[str]:
    """The codex command line that puts this one session on the bus.

    The server is configured with -c for this session only, so plain `codex`,
    and `codex exec` runs started from a bus session, stay off the bus.
    """
    agent = cfg.require_agent()
    env = {
        "AGENT_BUS_SESSION": "1",
        "AGENT_BUS_CHANNEL": "codex",
        "AGENT_BUS_NAME": agent,
        "AGENT_BUS_DIR": str(cfg.bus_dir),
        "AGENT_BUS_CODEX_BIN": codex_bin,
    }
    if cfg.hub:
        env["AGENT_BUS_HUB"] = cfg.hub
    table = ", ".join(f"{key} = {json.dumps(value)}" for key, value in env.items())
    server = f"mcp_servers.{CODEX_SERVER}"
    overrides = {
        "command": json.dumps(sys.executable),
        "args": json.dumps(["-m", "agent_bus", "serve"]),
        "env": f"{{ {table} }}",
        "startup_timeout_sec": "60",
        "tool_timeout_sec": "330",
        "default_tools_approval_mode": '"approve"',
    }
    argv = [codex_bin, "--no-daemon"]
    for key, value in overrides.items():
        argv += ["-c", f"{server}.{key}={value}"]
    rest = list(extra[1:] if extra[:1] == ["--"] else extra)
    return [*argv, *rest, CODEX_OPENING.format(me=agent)]


def _codex(cfg: config.Config, extra: Sequence[str]) -> int:
    codex_bin = shutil.which("codex")
    if codex_bin is None:
        raise BusError("codex is not on PATH")
    os.execv(codex_bin, codex_argv(cfg, codex_bin, extra))


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 2
    if args.command == "hook":
        # Before config.load: a hook handles its own configuration errors and
        # always exits 0, so a misconfigured bus never blocks a turn.
        return hooks.run(args.event)
    if args.command == "admin":
        # The hub's owner, not an agent: no agent name or bus config needed.
        return admin.run(args)
    try:
        cfg = config.load(bus_dir=args.bus_dir, agent=args.agent)
        if args.command == "serve":
            # The server opens the backend itself and reports a broken hub
            # setup to the session instead of failing to start.
            import anyio

            from agent_bus.server import serve

            anyio.run(serve, cfg)
            return 0
        if args.command == "hub-token":
            return _hub_token(cfg)
        if args.command == "codex":
            return _codex(cfg, args.codex_args)
        backend = open_backend(cfg)
        if args.command == "tail":
            return _tail(backend.store, follow=args.follow, raw=args.json)
        if args.command == "sync":
            from agent_bus.remote import HubStore

            if not isinstance(backend.store, HubStore):
                raise BusError("sync needs a hub: set AGENT_BUS_HUB or the plugin's hub_url")
            print(f"{backend.store.sync()} new events")
            return 0
        bus = Bus(
            backend.store,
            cfg.require_agent(),
            registry=backend.registry,
            rate_limit=cfg.rate_limit,
            rate_window=cfg.rate_window,
        )
        if args.command == "send":
            body = sys.stdin.read() if args.body == "-" else args.body
            message = bus.send(args.to, body, kind=args.kind, reply_to=args.reply_to)
            print(message.id)
        elif args.command == "inbox":
            for message in bus.inbox(unread_only=not args.all):
                print(_dump(message) if args.json else format_event(message))
        elif args.command == "ack":
            for msg_id in args.ids:
                print(f"{msg_id} {'acked' if bus.ack(msg_id) else 'already acked'}")
        elif args.command == "peers":
            for peer in bus.peers():
                if args.json:
                    print(json.dumps(peer.__dict__))
                else:
                    state = "live" if peer.live else "offline"
                    print(f"{peer.name} {state} unread_by_them={peer.unread_by_them}")
        return 0
    except (BusError, ValueError) as exc:
        print(f"agent-bus: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
