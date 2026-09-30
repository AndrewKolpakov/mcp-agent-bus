"""Drive channel_spike.py over raw JSON-RPC stdio, without Claude Code.

Usage: python3 drive.py [protocolVersion] [mcp requirement]
Prints the negotiated protocol version, the declared capabilities, the tool
list and the first channel notification after a line lands in the inbox.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROTOCOL = sys.argv[1] if len(sys.argv) > 1 else "2025-06-18"
REQUIREMENT = sys.argv[2] if len(sys.argv) > 2 else None

tmp = Path(tempfile.mkdtemp(prefix="spike-"))
env = {**os.environ, "SPIKE_LOG": str(tmp / "spike.log"), "SPIKE_INBOX": str(tmp / "inbox.txt")}
SCRIPT = HERE / os.environ.get("SPIKE_SCRIPT", "channel_spike.py")
cmd = ["uv", "run", "--quiet", "--script", str(SCRIPT)]
if REQUIREMENT:
    cmd = ["uv", "run", "--quiet", "--no-project", "--with", REQUIREMENT, "python", str(SCRIPT)]

proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, text=True)
lines: queue.Queue[dict] = queue.Queue()


def pump() -> None:
    assert proc.stdout
    for raw in proc.stdout:
        lines.put(json.loads(raw))


threading.Thread(target=pump, daemon=True).start()


def send(msg: dict) -> None:
    assert proc.stdin
    proc.stdin.write(json.dumps(msg) + "\n")
    proc.stdin.flush()


def wait_for(pred, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            msg = lines.get(timeout=0.5)
        except queue.Empty:
            continue
        if pred(msg):
            return msg
    raise TimeoutError("no matching message")


try:
    send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": PROTOCOL, "capabilities": {},
        "clientInfo": {"name": "drive", "version": "0"}}})
    init = wait_for(lambda m: m.get("id") == 1, timeout=120)
    result = init.get("result", init)
    print("requested protocol:", PROTOCOL)
    print("negotiated protocol:", result.get("protocolVersion"))
    print("capabilities:", json.dumps(result.get("capabilities")))
    print("instructions present:", bool(result.get("instructions")))
    send({"jsonrpc": "2.0", "method": "notifications/initialized"})
    send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    tools = wait_for(lambda m: m.get("id") == 2)
    print("tools:", [t["name"] for t in tools["result"]["tools"]])
    time.sleep(0.5)
    with (tmp / "inbox.txt").open("a") as fh:
        fh.write("hello from drive\n")
    note = wait_for(lambda m: m.get("method") == "notifications/claude/channel", timeout=10)
    print("channel notification:", json.dumps(note))
    print("RESULT: OK")
finally:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    if proc.returncode not in (0, -15, None):
        assert proc.stderr
        print("stderr:", proc.stderr.read()[-2000:])
