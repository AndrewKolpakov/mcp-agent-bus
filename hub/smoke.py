"""Smoke test of a deployed hub: two teammates and an outsider, end to end.

Creates three throwaway agents and a team with IAM credentials, runs the
checks over the public endpoints, then deletes the agents' tokens so none
outlive the run. Tokens never leave this process.

    AWS_PROFILE=<profile> AWS_REGION=eu-central-1 \\
    uv run --with websockets --with 'botocore[crt]' python hub/smoke.py <stack>
"""

from __future__ import annotations

import asyncio
import json
import secrets
import sys
import time
from typing import Any

import boto3
import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from agent_bus.hub.table import Table, token_hash

checks: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: Any = "") -> None:
    checks.append((name, ok, str(detail)))
    print(f"{'ok  ' if ok else 'FAIL'} {name} {detail}", flush=True)


def outputs(stack: str) -> dict[str, str]:
    found = boto3.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in found.get("Outputs", [])}


async def recv(ws: Any, timeout: float = 10) -> dict[str, Any]:
    reply: dict[str, Any] = json.loads(await asyncio.wait_for(ws.recv(), timeout))
    return reply


async def main(stack: str) -> int:
    out = outputs(stack)
    table = Table(out["TableName"])
    tag = secrets.token_hex(3)
    alice, bob, eve, team = f"smoke-a-{tag}", f"smoke-b-{tag}", f"smoke-e-{tag}", f"smoke-{tag}"
    tokens = {name: table.create_agent(name) for name in (alice, bob, eve)}
    table.add_member(team, alice)
    table.add_member(team, bob)

    def api(agent: str) -> httpx.Client:
        return httpx.Client(
            base_url=out["HttpUrl"],
            headers={"authorization": f"Bearer {tokens[agent]}"},
            timeout=15,
        )

    try:
        async with connect(
            out["WsUrl"], subprotocols=["agent-bus", tokens[bob]], ping_interval=None
        ) as ws:  # type: ignore[list-item]
            check("websocket subprotocol echoed", ws.subprotocol == "agent-bus", ws.subprotocol)
            await ws.send(json.dumps({"action": "ping"}))
            pong = await recv(ws, 15)
            conn = pong.get("connection", "")
            check("ping answers with the connection id", pong.get("type") == "pong" and conn, pong)

            with api(bob) as b, api(alice) as a, api(eve) as e:
                claim = b.put("/session", json={"session": f"s-{tag}", "connection": conn})
                check("bob claims the channel", claim.status_code == 200, claim.status_code)
                peers = a.get("/peers").json()
                check("alice sees bob live", peers == [{"name": bob, "live": True}], peers)

                started = time.monotonic()
                sent = a.post("/events", json={"to": bob, "body": "smoke", "kind": "task"})
                record = sent.json()
                check(
                    "message stored with hub fields",
                    sent.status_code == 200 and record.get("from") == alice and record.get("seq"),
                    sent.status_code,
                )
                push = await recv(ws)
                ms = round((time.monotonic() - started) * 1000)
                check("push {seq} reaches bob", push == {"seq": record["seq"]}, f"{push} {ms} ms")

                lines = b.get("/events", params={"after": record["seq"] - 1}).text.splitlines()
                got = [json.loads(line) for line in lines]
                check("bob reads it by seq", got[:1] == [record], len(got))

                ack = a.post("/events", json={"type": "ack", "id": record["id"]})
                check("sender cannot ack it", ack.status_code == 403, ack.status_code)
                ack = b.post("/events", json={"type": "ack", "id": record["id"]})
                check(
                    "bob acks it",
                    ack.status_code == 200 and ack.json()["by"] == bob,
                    ack.status_code,
                )

                refused = e.post("/events", json={"to": bob, "body": "hi"})
                check("outsider refused", refused.status_code == 403, refused.json())
                spied = e.get("/events", params={"after": 0}).text
                check("outsider sees nothing", spied == "", len(spied))
                dup = e.post("/events", json={"to": bob, "body": "x", "id": record["id"]})
                check("outsider cannot reuse an id", dup.status_code in (403, 409), dup.status_code)

                stranger = httpx.get(
                    out["HttpUrl"] + "/peers", headers={"authorization": "Bearer ab_nope"}
                )
                check(
                    "bad HTTP token refused",
                    stranger.status_code in (401, 403),
                    stranger.status_code,
                )
                release = b.request("DELETE", "/session", json={"session": f"s-{tag}"})
                check(
                    "bob releases the channel", release.json() == {"released": True}, release.text
                )

        try:
            async with connect(
                out["WsUrl"], subprotocols=["agent-bus", "ab_nope"], ping_interval=None
            ):  # type: ignore[list-item]
                check("bad websocket token refused", False, "connected")
        except InvalidStatus as exc:
            status = exc.response.status_code
            check("bad websocket token refused", status == 401, status)
    finally:
        for token in tokens.values():
            table.client.delete_item(
                TableName=table.name,
                Key={"pk": {"S": f"TOKEN#{token_hash(token)}"}, "sk": {"S": "AGENT"}},
            )
        print(f"revoked the tokens of {', '.join(tokens)}")

    failed = [name for name, ok, _ in checks if not ok]
    print(f"{len(checks) - len(failed)}/{len(checks)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "agent-bus-dev")))
