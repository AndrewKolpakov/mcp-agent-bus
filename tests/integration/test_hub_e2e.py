"""Two `agent-bus serve` processes on one machine, talking through a deployed hub.

Skipped unless AGENT_BUS_TEST_HUB_STACK names a deployed hub stack and AWS
credentials that can write its table are in the environment. Each run
makes two fresh agents on a fresh team and revokes their tokens at the end.

    AWS_PROFILE=<profile> AWS_REGION=eu-central-1 AGENT_BUS_TEST_HUB_STACK=agent-bus-dev \\
    uv run --with 'botocore[crt]' pytest tests/integration/test_hub_e2e.py
"""

from __future__ import annotations

import json
import os
import secrets
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from agent_bus.remote import TOKEN_NAME, hub_home, write_token

from .mcp_client import ServerProcess, tool_json

STACK = os.environ.get("AGENT_BUS_TEST_HUB_STACK", "")

pytestmark = pytest.mark.skipif(not STACK, reason="AGENT_BUS_TEST_HUB_STACK is not set")


@pytest.fixture
def hub(tmp_path: Path) -> Iterator[tuple[str, dict[str, str], Path]]:
    import boto3

    from agent_bus.hub.table import Table, token_hash

    stack = boto3.client("cloudformation").describe_stacks(StackName=STACK)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
    url, table = outputs["HttpUrl"], Table(outputs["TableName"])
    tag = secrets.token_hex(3)
    names = {"alice": f"e2e-a-{tag}", "bob": f"e2e-b-{tag}"}
    tokens = {name: table.create_agent(name) for name in names.values()}
    for name in names.values():
        table.add_member(f"e2e-{tag}", name)
    bus_dir = tmp_path / "bus"
    for name, token in tokens.items():
        write_token(hub_home(bus_dir, url, name) / TOKEN_NAME, token)
    try:
        yield url, names, bus_dir
    finally:
        for token in tokens.values():
            table.client.delete_item(
                TableName=table.name,
                Key={"pk": {"S": f"TOKEN#{token_hash(token)}"}, "sk": {"S": "AGENT"}},
            )


def test_task_and_result_cross_the_hub_between_two_sessions(
    hub: tuple[str, dict[str, str], Path],
) -> None:
    url, names, bus_dir = hub
    servers: list[ServerProcess] = []
    try:
        for role in ("alice", "bob"):
            env = {
                "AGENT_BUS_SESSION": "1",
                "AGENT_BUS_NAME": names[role],
                "AGENT_BUS_HUB": url,
                "AGENT_BUS_SETTLE_SECONDS": "0",
            }
            servers.append(ServerProcess(bus_dir, env))
        alice, bob = servers
        for role, server in zip(("alice", "bob"), servers, strict=True):
            instructions = server.connect()["result"]["instructions"]
            assert f'You are agent "{names[role]}"' in instructions
            assert "Another session" not in instructions, "the channel was not claimed"

        started = time.monotonic()
        sent = tool_json(
            alice.call("send", {"to": names["bob"], "body": "e2e task", "kind": "task"})
        )
        task = bob.channel_events(timeout=30)["params"]
        task_ms = round((time.monotonic() - started) * 1000)
        assert task["content"] == "e2e task"
        assert task["meta"]["from"] == names["alice"]
        assert task["meta"]["msg_id"] == sent["msg_id"]

        started = time.monotonic()
        tool_json(
            bob.call(
                "send",
                {
                    "to": names["alice"],
                    "body": "e2e result",
                    "kind": "result",
                    "reply_to": sent["msg_id"],
                },
            )
        )
        result = alice.channel_events(timeout=30)["params"]
        result_ms = round((time.monotonic() - started) * 1000)
        assert result["content"] == "e2e result"
        assert result["meta"]["thread"] == sent["thread"]

        peers = tool_json(alice.call("peers"))
        assert peers == [{"name": names["bob"], "live": True, "unread_by_them": 0}]

        # Both pushes were recorded as delivered, and the reply acked the task.
        deadline = time.monotonic() + 20
        events: list[dict[str, object]] = []
        kinds: list[str] = []
        mirror = hub_home(bus_dir, url, names["alice"]) / "log.jsonl"
        while time.monotonic() < deadline:
            events = [json.loads(line) for line in mirror.read_text().splitlines()]
            kinds = sorted(str(e["type"]) for e in events)
            if kinds == ["ack", "delivered", "delivered", "message", "message"]:
                break
            time.sleep(0.5)
        assert kinds == ["ack", "delivered", "delivered", "message", "message"]
        seqs = [int(str(e["seq"])) for e in events]
        assert seqs == sorted(seqs)
        print(f"task pushed in {task_ms} ms, result in {result_ms} ms")
    finally:
        for server in servers:
            server.stop()
            assert server.bad_lines == [], f"non-JSON on stdout: {server.bad_lines}"
