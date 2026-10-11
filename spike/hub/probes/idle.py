"""Q7: what keeps an idle WebSocket connection open past the 10-minute timeout.

Three connections at once, each opened with one app-level ping to learn its
connection id:
- silent: nothing after that
- proto:  a protocol ping frame every PERIOD seconds
- app:    an app-level {"action": "ping"} message every PERIOD seconds
GetConnection's LastActiveAt is sampled for each, to see which kind of
traffic counts as activity.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from common import emit, http, state, ws_url

PERIOD = 240.0
DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 780.0


async def whoami(ws) -> str:
    await ws.send(json.dumps({"action": "ping"}))
    reply = json.loads(await asyncio.wait_for(ws.recv(), 15))
    return reply["connection"]


async def sample(client: httpx.Client, name: str, conn: str) -> None:
    r = await asyncio.to_thread(client.get, f"/debug/conn/{conn}")
    emit(probe="idle", conn=name, sample=r.json())


async def run(name: str, client: httpx.Client) -> None:
    token = state()["tokens"]["alice"]
    started = time.monotonic()
    async with connect(ws_url(token=token), ping_interval=None, ping_timeout=None) as ws:
        conn = await whoami(ws)
        emit(probe="idle", conn=name, id=conn, event="open")
        await sample(client, name, conn)
        next_ping = PERIOD
        try:
            while time.monotonic() - started < DURATION:
                await asyncio.sleep(30)
                elapsed = time.monotonic() - started
                if elapsed >= next_ping:
                    next_ping += PERIOD
                    if name == "proto":
                        t = time.monotonic()
                        waiter = await ws.ping()
                        try:
                            await asyncio.wait_for(waiter, 10)
                            emit(probe="idle", conn=name, event="pong", ms=round((time.monotonic() - t) * 1000))
                        except TimeoutError:
                            emit(probe="idle", conn=name, event="no pong in 10s")
                    elif name == "app":
                        await ws.send(json.dumps({"action": "ping"}))
                        reply = await asyncio.wait_for(ws.recv(), 15)
                        emit(probe="idle", conn=name, event="app pong", reply=json.loads(reply).get("pong"))
                    await asyncio.sleep(2)
                    await sample(client, name, conn)
                # Detect a close without blocking on recv forever.
                if ws.close_code is not None:
                    break
            if ws.close_code is None:
                emit(probe="idle", conn=name, event="survived", seconds=round(time.monotonic() - started))
                return
        except ConnectionClosed:
            pass
        emit(
            probe="idle",
            conn=name,
            event="closed",
            seconds=round(time.monotonic() - started),
            code=ws.close_code,
            reason=ws.close_reason,
        )


async def main() -> None:
    with http("alice") as client:
        await asyncio.gather(*(run(name, client) for name in ("silent", "proto", "app")))


asyncio.run(main())
