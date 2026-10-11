"""The happy path: a recipient's open socket gets {"seq": N} for each message.

The recipient connects, a sender posts ROUNDS messages, and each push is
matched to its POST to measure time from send to wake-up. Then the
recipient catches up with GET /events?after= and checks it got every body.
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time

from websockets.asyncio.client import connect

from common import emit, http, state, ws_url

SENDER, RECIPIENT = "dave", "carol"
ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 10


async def main() -> None:
    token = state()["tokens"][RECIPIENT]
    async with connect(ws_url(token=token), ping_interval=None) as ws:
        await ws.send(json.dumps({"action": "ping"}))
        await asyncio.wait_for(ws.recv(), 15)
        latencies, seqs = [], []
        with http(SENDER) as sender:
            for i in range(ROUNDS):
                t = time.monotonic()
                r = (await asyncio.to_thread(sender.post, "/events", json={"to": RECIPIENT, "body": f"push {i}"})).json()
                push = json.loads(await asyncio.wait_for(ws.recv(), 10))
                latencies.append(round((time.monotonic() - t) * 1000))
                seqs.append(push["seq"])
                assert push["seq"] == r["event"]["seq"], (push, r["event"]["seq"])
        with http(RECIPIENT) as me:
            lines = [
                json.loads(line)
                for line in me.get("/events", params={"after": seqs[0] - 1, "limit": 1000}).text.splitlines()
            ]
        got = [e["seq"] for e in lines if e["seq"] in set(seqs)]
        emit(
            probe="push",
            rounds=ROUNDS,
            send_to_wake_ms_p50=statistics.median(latencies),
            send_to_wake_ms_max=max(latencies),
            caught_up=got == seqs,
        )


asyncio.run(main())
