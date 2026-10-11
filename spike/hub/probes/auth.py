"""Q8: how a client proves its identity on $connect, and how fast revocation bites.

Cases: token in the query string, token in Sec-WebSocket-Protocol, a
single-use ticket fetched over HTTP, a reused ticket, a bad token, no
credential. Then the agent named on the command line has its TOKEN# row
deleted, and WebSocket connects and HTTP calls are retried until refused.

Usage: auth.py <revocable-agent>
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import time

import boto3
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from common import emit, http, state, ws_url

REVOKE = sys.argv[1]


async def attempt(case: str, url: str, subprotocols: list[str] | None = None) -> bool:
    try:
        async with connect(url, subprotocols=subprotocols, ping_interval=None) as ws:  # type: ignore[arg-type]
            await ws.send(json.dumps({"action": "ping"}))
            reply = json.loads(await asyncio.wait_for(ws.recv(), 15))
            emit(probe="auth", case=case, ok=True, agent=reply.get("agent"), subprotocol=ws.subprotocol)
            return True
    except InvalidStatus as exc:
        emit(probe="auth", case=case, ok=False, status=exc.response.status_code)
        return False


async def main() -> None:
    tokens = state()["tokens"]
    await attempt("query token", ws_url(token=tokens["alice"]))
    await attempt("subprotocol", ws_url(), ["agent-bus", tokens["alice"]])
    with http("alice") as client:
        ticket = client.post("/ticket").json()["ticket"]
    await attempt("ticket", ws_url(ticket=ticket))
    await attempt("ticket reused", ws_url(ticket=ticket))
    await attempt("bad token", ws_url(token="tok_nope"))
    await attempt("no credential", ws_url())

    # Revocation: warm the HTTP authorizer cache, then delete the token row.
    token = tokens[REVOKE]
    with http(REVOKE) as client:
        emit(probe="auth", case="http before revoke", status=client.get("/events").status_code)
        boto3.client("dynamodb").delete_item(
            TableName=state()["table"],
            Key={"pk": {"S": f"TOKEN#{hashlib.sha256(token.encode()).hexdigest()}"}, "sk": {"S": "AGENT"}},
        )
        revoked = time.monotonic()
        await attempt("ws right after revoke", ws_url(token=token))
        while True:
            status = client.get("/events").status_code
            elapsed = round(time.monotonic() - revoked, 1)
            emit(probe="auth", case="http after revoke", seconds=elapsed, status=status)
            if status != 200 or elapsed > 400:
                break
            await asyncio.sleep(10)


os.environ.setdefault("AWS_REGION", "eu-central-1")
asyncio.run(main())
