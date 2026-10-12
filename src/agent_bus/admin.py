"""`agent-bus admin`: agents, tokens and teams on a hub, run by its owner.

It writes the hub's table directly with the caller's AWS credentials, so
the hub's API has no admin routes to protect. A token is printed once, to
stdout, when it is made; the table keeps only its hash.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agent_bus.hub.table import Table

STACK_ENV = "AGENT_BUS_ADMIN_STACK"
FOLLOW_SECONDS = 2.0


def add_parser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    admin = sub.add_parser(
        "admin", help="manage a hub's agents, tokens and teams (AWS credentials)"
    )
    where = admin.add_mutually_exclusive_group()
    where.add_argument("--stack", help=f"hub stack name (default: ${STACK_ENV})")
    where.add_argument("--table", help="hub table name, instead of a stack")
    groups = admin.add_subparsers(dest="group", metavar="<group>", required=True)

    agent = groups.add_parser("agent", help="agents").add_subparsers(
        dest="action", metavar="<action>", required=True
    )
    add = agent.add_parser("add", help="register an agent and print its token")
    add.add_argument("name")
    add.add_argument("--team", action="append", default=[], help="also add it to this team")
    agent.add_parser("list", help="agents and their teams")
    remove = agent.add_parser("remove", help="delete an agent, its token, teams and view")
    remove.add_argument("name")
    remove.add_argument("--yes", action="store_true", help="confirm; the view cannot be restored")

    token = groups.add_parser("token", help="tokens").add_subparsers(
        dest="action", metavar="<action>", required=True
    )
    token.add_parser("rotate", help="issue a new token; the old one stops working").add_argument(
        "name"
    )
    token.add_parser("revoke", help="leave the agent without a token").add_argument("name")

    team = groups.add_parser("team", help="teams").add_subparsers(
        dest="action", metavar="<action>", required=True
    )
    team.add_parser("list", help="teams and their members")

    member = groups.add_parser("member", help="team membership").add_subparsers(
        dest="action", metavar="<action>", required=True
    )
    for action, text in (("add", "add agents to a team"), ("remove", "take agents off a team")):
        parser = member.add_parser(action, help=text)
        parser.add_argument("team")
        parser.add_argument("agents", nargs="+", metavar="agent")

    tail = groups.add_parser("tail", help="the hub's whole log, every agent's events")
    tail.add_argument("-n", type=int, default=20, help="events to show (default 20)")
    tail.add_argument("-f", "--follow", action="store_true", help="keep printing new events")
    tail.add_argument("--json", action="store_true", help="raw log lines")
    tail.set_defaults(action="tail")


def open_table(args: argparse.Namespace) -> Table:
    try:
        import boto3
    except ImportError:
        raise SystemExit(
            "agent-bus admin needs boto3: run it from the repository with `uv run`,"
            " or install agent-bus[admin]"
        ) from None
    from agent_bus.hub.table import Table

    if args.table:
        return Table(args.table)
    stack = args.stack or os.environ.get(STACK_ENV)
    if not stack:
        raise SystemExit(f"agent-bus admin: name the hub with --stack, --table or ${STACK_ENV}")
    found = boto3.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in found.get("Outputs", [])}
    return Table(outputs["TableName"])


def run(args: argparse.Namespace, table: Table | None = None) -> int:
    try:
        return _run(args, table or open_table(args))
    except (LookupError, ValueError) as exc:
        print(f"agent-bus admin: {exc}", file=sys.stderr)
        return 1


def _run(args: argparse.Namespace, table: Table) -> int:
    from agent_bus.cli import _dump, format_event

    what = (args.group, args.action)
    if what == ("agent", "add"):
        token = table.create_agent(args.name)
        for team in args.team:
            table.add_member(team, args.name)
        print(token)
        print(
            f"agent {args.name} created; the token above is shown once."
            " Hand it over privately: `agent-bus hub-token` reads it from stdin.",
            file=sys.stderr,
        )
    elif what == ("agent", "list"):
        for name in table.agents():
            print(f"{name} teams={','.join(sorted(table.teams(name))) or '-'}")
    elif what == ("agent", "remove"):
        if not args.yes:
            print(
                f"agent-bus admin: removing {args.name} deletes its view; pass --yes",
                file=sys.stderr,
            )
            return 1
        table.remove_agent(args.name)
        print(f"removed {args.name}")
    elif what == ("token", "rotate"):
        print(table.rotate_token(args.name))
        print(
            f"new token for {args.name}; the old one is refused now on WebSocket"
            " and within 60 s over HTTP",
            file=sys.stderr,
        )
    elif what == ("token", "revoke"):
        done = table.revoke_token(args.name)
        print(f"revoked the token of {args.name}" if done else f"{args.name} had no token")
    elif what == ("team", "list"):
        for team in table.all_teams():
            print(f"{team}: {' '.join(sorted(table.members(team))) or '-'}")
    elif what[0] == "member":
        for agent in args.agents:
            if args.action == "add":
                table.add_member(args.team, agent)
            else:
                table.remove_member(args.team, agent)
        verb = "added to" if args.action == "add" else "removed from"
        print(f"{' '.join(args.agents)} {verb} {args.team}")
    elif what == ("tail", "tail"):
        events = table.log_tail(args.n)
        for event in events:
            print(_dump(event) if args.json else format_event(event), flush=True)
        last = (events[-1].seq or 0) if events else 0
        while args.follow:
            time.sleep(FOLLOW_SECONDS)
            for event in table.log_after(last):
                print(_dump(event) if args.json else format_event(event), flush=True)
                last = event.seq or last
    return 0
