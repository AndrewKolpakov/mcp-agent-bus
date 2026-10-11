"""Shared helpers for the stage 7 probes.

State (URLs and raw tokens) lives in a JSON file outside the repo, named by
SPIKE_STATE. Raw tokens never go into the table or the repository.
"""

from __future__ import annotations

import json
import os
import ssl
import time
from pathlib import Path
from typing import Any

import httpx

STATE = Path(os.environ["SPIKE_STATE"])


def state() -> dict[str, Any]:
    return json.loads(STATE.read_text())


def http(agent: str) -> httpx.Client:
    s = state()
    return httpx.Client(
        base_url=s["http_url"],
        headers={"authorization": f"Bearer {s['tokens'][agent]}"},
        timeout=30,
    )


def ws_url(**query: str) -> str:
    q = "&".join(f"{k}={v}" for k, v in query.items())
    return f"{state()['ws_url']}?{q}" if q else state()["ws_url"]


def ts() -> str:
    return time.strftime("%H:%M:%S")


def emit(**fields: Any) -> None:
    print(json.dumps({"t": ts(), **fields}, default=str), flush=True)


SSL = ssl.create_default_context()
