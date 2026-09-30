"""Command-line entry point: `agent-bus <command>`.

The MCP server and every hook run through this one entry point, so the
plugin needs a single executable on the bus.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from agent_bus import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-bus",
        description="Message bus between Claude Code sessions on different accounts.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_subparsers(dest="command", metavar="<command>")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 2
    parser.error(f"unknown command: {args.command}")
