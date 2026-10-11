"""Q9: can GetConnection tell a live connection from a gone one?

- killed:  the client process gets SIGKILL; the kernel closes the socket.
- stopped: the client process gets SIGSTOP; the TCP connection stays up
           but nobody reads it, like a laptop that went to sleep.
For each, GetConnection and post_to_connection are polled through the
spike's debug routes until the hub reports the connection gone.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from common import emit, http

HERE = Path(__file__).parent
DURATION = float(sys.argv[1]) if len(sys.argv) > 1 else 780.0
# "post" pushes to the stopped client on every poll; "quiet" only asks
# GetConnection, which does not count as activity.
MODE = sys.argv[2] if len(sys.argv) > 2 else "post"
AGENT = sys.argv[3] if len(sys.argv) > 3 else "bob"


def spawn(agent: str) -> tuple[subprocess.Popen[str], str]:
    proc = subprocess.Popen(
        [sys.executable, str(HERE / "holder.py"), agent],
        stdout=subprocess.PIPE,
        text=True,
        env=os.environ,
    )
    assert proc.stdout is not None
    conn = proc.stdout.readline().strip()
    return proc, conn


def watch(case: str, conn: str, interval: float, limit: float, *, post: bool) -> float | None:
    started = time.monotonic()
    with http("alice") as client:
        while time.monotonic() - started < limit:
            got = client.get(f"/debug/conn/{conn}").json()
            fields = {"outcome": got.get("outcome"), "last_active": got.get("LastActiveAt")}
            if post:
                fields["post"] = client.post(f"/debug/conn/{conn}").json().get("outcome")
            elapsed = round(time.monotonic() - started, 1)
            emit(probe="sleeper", case=case, seconds=elapsed, **fields)
            if got.get("outcome") != "ok" or got.get("error"):
                return elapsed
            time.sleep(interval)
    return None


# Killed: the kernel sends FIN/RST, so this is the easy case.
if MODE == "post":
    proc, conn = spawn(AGENT)
    emit(probe="sleeper", case="killed", id=conn, event="open")
    proc.kill()
    gone = watch("killed", conn, 0.5, 60, post=False)
    emit(probe="sleeper", case="killed", event="result", gone_after=gone)

# Stopped: the connection stays established but silent.
proc, conn = spawn(AGENT)
case = f"stopped/{MODE}"
emit(probe="sleeper", case=case, id=conn, event="open")
os.kill(proc.pid, signal.SIGSTOP)
gone = watch(case, conn, 30 if MODE == "post" else 60, DURATION, post=MODE == "post")
emit(probe="sleeper", case=case, event="result", gone_after=gone)
os.kill(proc.pid, signal.SIGCONT)
try:
    out, _ = proc.communicate(timeout=20)
    emit(probe="sleeper", case=case, event="after SIGCONT", holder_output=out[-1500:])
except subprocess.TimeoutExpired:
    proc.kill()
    out, _ = proc.communicate()
    emit(probe="sleeper", case=case, event="still open after SIGCONT, killed", holder_output=out[-1500:])
