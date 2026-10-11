"""A client that connects as an agent, prints its connection id, then idles.

Used by sleeper.py, which kills or stops this process. Pushes it receives
are printed so a resumed process shows what was buffered while it slept.
"""

from __future__ import annotations

import asyncio
import json
import sys

from websockets.asyncio.client import connect

from common import emit, state, ws_url


async def main(agent: str) -> None:
    token = state()["tokens"][agent]
    async with connect(ws_url(token=token), ping_interval=None, ping_timeout=None) as ws:
        await ws.send(json.dumps({"action": "ping"}))
        reply = json.loads(await asyncio.wait_for(ws.recv(), 15))
        print(reply["connection"], flush=True)
        async for message in ws:
            emit(probe="holder", received=message)
        emit(probe="holder", event="closed", code=ws.close_code, reason=ws.close_reason)


asyncio.run(main(sys.argv[1]))
