"""Command-line entry point: `agent-bus <command>`.

The MCP server and every hook run through this one entry point, so the
plugin needs a single executable on the bus.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence

from agent_bus import __version__, config, hooks
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
    try:
        cfg = config.load(bus_dir=args.bus_dir, agent=args.agent)
        backend = open_backend(cfg)
        if args.command == "tail":
            return _tail(backend.store, follow=args.follow, raw=args.json)
        if args.command == "serve":
            import anyio

            from agent_bus.server import serve

            anyio.run(serve, cfg)
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
