"""Append the hook's stdin JSON to hooks.log next to this file (spike only)."""

import json
import sys
import time
from pathlib import Path

payload = sys.stdin.read()
try:
    data = json.loads(payload)
except json.JSONDecodeError:
    data = {"raw": payload}
with (Path(__file__).resolve().parent / "hooks.log").open("a", encoding="utf-8") as fh:
    fh.write(json.dumps({"t": time.time(), "argv": sys.argv[1:], "input": data}) + "\n")
