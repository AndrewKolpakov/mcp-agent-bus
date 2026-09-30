"""A raw JSON-RPC stdio client for driving `agent-bus serve` in tests.

The SDK client is not used on purpose: it may drop a notification whose
method is not in the MCP spec, and `notifications/claude/channel` is exactly
such a method. Every stdout line must parse as JSON; anything else fails the
test, because a stray line breaks a real Claude Code session.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

# What Claude Code 2.1.285 sent in the stage-0 live runs.
DISCOVER_PROBE: dict[str, Any] = {
    "jsonrpc": "2.0",
    "id": "server-discover-probe-1",
    "method": "server/discover",
    "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28"}},
}
CLIENT_PROTOCOL = "2025-11-25"


class ServerProcess:
    def __init__(self, bus_dir: Path, env: Mapping[str, str]) -> None:
        full_env = {
            k: v for k, v in os.environ.items() if not k.startswith(("AGENT_BUS", "CLAUDE_PLUGIN"))
        }
        full_env.update({"AGENT_BUS_DIR": str(bus_dir), **env})
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "agent_bus", "serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=full_env,
            text=True,
        )
        self.messages: queue.Queue[dict[str, Any]] = queue.Queue()
        self.bad_lines: list[str] = []
        self._next_id = 1
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            try:
                self.messages.put(json.loads(line))
            except json.JSONDecodeError:
                self.bad_lines.append(line)

    def send(self, message: Mapping[str, Any]) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(message) + "\n")
        self.proc.stdin.flush()

    def wait_for(
        self, predicate: Callable[[dict[str, Any]], bool], timeout: float = 20.0
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                message = self.messages.get(timeout=0.2)
            except queue.Empty:
                continue
            if predicate(message):
                return message
        raise TimeoutError(f"no matching message within {timeout}s; stderr: {self.stderr_tail()}")

    def collect(
        self, predicate: Callable[[dict[str, Any]], bool], seconds: float
    ) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                message = self.messages.get(timeout=0.1)
            except queue.Empty:
                continue
            if predicate(message):
                found.append(message)
        return found

    def request(self, method: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        request_id = self._next_id
        self._next_id += 1
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params
        self.send(payload)
        return self.wait_for(lambda m: m.get("id") == request_id)

    def handshake(self) -> dict[str, Any]:
        self.send(DISCOVER_PROBE)
        self.wait_for(lambda m: m.get("id") == DISCOVER_PROBE["id"])
        init = self.request(
            "initialize",
            {
                "protocolVersion": CLIENT_PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "agent-bus-tests", "version": "0"},
            },
        )
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return init

    def call(self, tool: str, arguments: Mapping[str, Any] | None = None) -> dict[str, Any]:
        response = self.request("tools/call", {"name": tool, "arguments": dict(arguments or {})})
        return response["result"]  # type: ignore[no-any-return]

    def channel_events(self, timeout: float = 20.0) -> dict[str, Any]:
        return self.wait_for(lambda m: m.get("method") == "notifications/claude/channel", timeout)

    def close_stdin(self) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.close()

    def stderr_tail(self) -> str:
        if self.proc.poll() is None or self.proc.stderr is None:
            return "<running>"
        tail: str = self.proc.stderr.read()[-2000:]
        return tail

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


def tool_json(result: Mapping[str, Any]) -> Any:
    assert not result.get("isError"), result
    return json.loads(result["content"][0]["text"])
