"""Q11: the conditional-put sequencer under concurrent writers.

WRITERS threads, each posting PER_WRITER messages as fast as it can. Then:
- every returned seq is unique and the new range has no holes
- retry counts per POST (attempts, LOG collisions, transaction conflicts)
- one POST repeated with the same id returns the same seq (idempotency)
"""

from __future__ import annotations

import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from agent_bus.models import new_id

from common import emit, http

WRITERS = int(sys.argv[1]) if len(sys.argv) > 1 else 10
PER_WRITER = int(sys.argv[2]) if len(sys.argv) > 2 else 20
SENDERS = ("carol", "dave", "erin")


def writer(n: int) -> list[dict]:
    sender = SENDERS[n % len(SENDERS)]
    to = SENDERS[(n + 1) % len(SENDERS)]
    out = []
    with http(sender) as client:
        for i in range(PER_WRITER):
            t = time.monotonic()
            r = client.post("/events", json={"to": to, "body": f"race {n}/{i}"})
            r.raise_for_status()
            body = r.json()
            out.append({**body["stats"], "seq": body["event"]["seq"], "rtt_ms": round((time.monotonic() - t) * 1000)})
    return out


started = time.monotonic()
with ThreadPoolExecutor(WRITERS) as pool:
    results = [r for chunk in pool.map(writer, range(WRITERS)) for r in chunk]
wall = time.monotonic() - started

seqs = sorted(r["seq"] for r in results)
holes = sorted(set(range(seqs[0], seqs[-1] + 1)) - set(seqs))
attempts = [r["attempts"] for r in results]
emit(
    probe="race",
    writers=WRITERS,
    posts=len(results),
    wall_s=round(wall, 1),
    unique=len(set(seqs)) == len(seqs),
    seq_range=[seqs[0], seqs[-1]],
    holes_in_range=len(holes),
    attempts_mean=round(statistics.mean(attempts), 2),
    attempts_max=max(attempts),
    retried_posts=sum(a > 1 for a in attempts),
    log_collisions=sum(r["log_collisions"] for r in results),
    conflicts=sum(r["conflicts"] for r in results),
    append_ms_p50=statistics.median(r["append_ms"] for r in results),
    append_ms_max=max(r["append_ms"] for r in results),
    rtt_ms_p50=statistics.median(r["rtt_ms"] for r in results),
)

# Idempotency: the same client id twice gives one event.
msg_id = new_id()
with http("carol") as client:
    first = client.post("/events", json={"id": msg_id, "to": "dave", "body": "once"}).json()
    second = client.post("/events", json={"id": msg_id, "to": "dave", "body": "once"}).json()
emit(
    probe="race",
    idempotent=first["event"]["seq"] == second["event"]["seq"],
    first_seq=first["event"]["seq"],
    second=second["stats"],
)
