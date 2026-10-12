"""The hub client: an `EventStore` and a `SessionRegistry` backed by a hub.

Reads never touch the network. The agent's view lives in a local mirror,
`<bus_dir>/hubs/<hub host>/<agent>/log.jsonl`, in the same JSONL format as
the local log, so hooks, the CLI and the server read it with the local
code. Only `HubStore.sync` writes the mirror, and only from `GET /events`:
under the log's `flock` it reads the mirror's last seq, then appends the
hub's lines above it verbatim. Two processes syncing at once therefore
never duplicate or reorder a line, and the mirror's line order is seq order,
which is what replay in state.py relies on.

Writes go to the hub, which answers with the stored event; the mirror sees
it only on the next sync. `HubLink` keeps a WebSocket open, pings every
`PING_SECONDS` (that is both the keepalive and the channel claim's liveness)
and syncs whenever the hub pushes `{"seq": N}`, after every (re)connect,
and on every ping as a fallback for a lost push.

The hub module (`agent_bus.hub`) runs in Lambda and must never import this
one: httpx and websockets are not in its bundle.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import stat
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

import anyio
import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from agent_bus import __version__
from agent_bus.bus import BusError, HubUnavailableError, RateLimitError
from agent_bus.models import EVENT_ADAPTER, Event, dump_event
from agent_bus.store import LOG_NAME, ReadResult, Store

log = logging.getLogger(__name__)

SUBPROTOCOL = "agent-bus"
TOKEN_NAME = "token"
PAGE = 500
PING_SECONDS = 60.0
PEERS_TTL_SECONDS = 5.0
TAIL_BYTES = 64 * 1024
"""Block size for reading the mirror backwards to find its last seq."""
MAX_BACKOFF_SECONDS = 60.0


def hub_home(bus_dir: Path, hub: str, agent: str) -> Path:
    """Where this agent's mirror and token live for `hub`."""
    host = urlparse(hub).hostname or hub
    return bus_dir / "hubs" / host / agent


def read_token(path: Path) -> str:
    try:
        mode = path.stat().st_mode
        token = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        raise BusError(f"no hub token at {path}; see docs/hub.md") from None
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise BusError(f"hub token {path} is readable by others; run chmod 600 on it")
    if not token:
        raise BusError(f"hub token file {path} is empty")
    return token


def write_token(path: Path, token: str) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(token.strip() + "\n")
    path.chmod(0o600)


class HubApi:
    def __init__(
        self,
        url: str,
        token: str,
        *,
        timeout: float = 15.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.url = url
        self.client = httpx.Client(
            base_url=url,
            headers={"authorization": f"Bearer {token}", "user-agent": f"agent-bus/{__version__}"},
            timeout=timeout,
            transport=transport,
        )

    def _call(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            response = self.client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise HubUnavailableError(f"hub unreachable: {exc}") from exc
        status = response.status_code
        if status < 400:
            return response
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        # The hub answers {"error": ...}; API Gateway itself answers {"message": ...}.
        error = payload.get("error") if isinstance(payload, dict) else None
        if error is None:
            if status in (401, 403):
                raise BusError("the hub refused this agent's token; it may have been revoked")
            raise HubUnavailableError(f"hub answered {status} to {method} {path}")
        if status == 429:
            raise RateLimitError(error)
        raise BusError(error)

    def hello(self) -> dict[str, Any]:
        reply: dict[str, Any] = self._call("GET", "/hello").json()
        return reply

    def post_event(self, record: dict[str, Any]) -> dict[str, Any]:
        reply: dict[str, Any] = self._call("POST", "/events", json=record).json()
        return reply

    def events(self, after: int, limit: int = PAGE) -> list[bytes]:
        response = self._call("GET", "/events", params={"after": after, "limit": limit})
        return [line for line in response.content.split(b"\n") if line.strip()]

    def peers(self) -> list[dict[str, Any]]:
        reply: list[dict[str, Any]] = self._call("GET", "/peers").json()
        return reply

    def put_session(self, session: str, connection: str) -> bool:
        """Claim or keep the channel; False when another live session holds it."""
        try:
            self._call("PUT", "/session", json={"session": session, "connection": connection})
        except HubUnavailableError:
            raise
        except BusError as exc:
            log.info("channel claim refused: %s", exc)
            return False
        return True

    def delete_session(self, session: str) -> None:
        self._call("DELETE", "/session", json={"session": session})


class HubStore:
    def __init__(self, api: HubApi, home: Path) -> None:
        self.api = api
        self.home = home
        self.mirror = Store(home)

    def ensure(self) -> None:
        self.mirror.ensure()

    def append[E: Event](self, event: E) -> E:
        record = json.loads(dump_event(event))
        stored = EVENT_ADAPTER.validate_python(self.api.post_event(record))
        if stored.type != event.type:
            raise HubUnavailableError(f"hub stored a {stored.type} for a {event.type}")
        return cast(E, stored)

    def size(self) -> int:
        return self.mirror.size()

    def read_from(self, offset: int = 0) -> ReadResult:
        return self.mirror.read_from(offset)

    def read_all(self) -> ReadResult:
        return self.mirror.read_all()

    def sync(self) -> int:
        """Append the hub's events above the mirror's last seq; return how many."""
        self.ensure()
        fd = os.open(self.home / LOG_NAME, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                return self._sync_locked(fd)
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _sync_locked(self, fd: int) -> int:
        size = os.fstat(fd).st_size
        if size and os.pread(fd, 1, size - 1) != b"\n":
            # A torn last line from a crash: end it, so the next line is not
            # glued to it. Readers skip the fragment as unreadable.
            os.write(fd, b"\n")
            size += 1
        last = _last_seq(fd, size)
        added = 0
        while True:
            fresh = 0
            for line in self.api.events(after=last):
                seq = json.loads(line).get("seq")
                if not isinstance(seq, int) or seq <= last:
                    continue
                written = os.write(fd, line + b"\n")
                if written != len(line) + 1:
                    raise OSError(f"short write to the mirror: {written}/{len(line) + 1} bytes")
                last = seq
                fresh += 1
            if not fresh:
                return added
            added += fresh


def _last_seq(fd: int, size: int) -> int:
    """The seq of the last whole line that has one; 0 for an empty mirror.

    The mirror ends with a newline here, so lines are walked back from the
    end one whole line at a time; a line longer than TAIL_BYTES is read in
    several blocks but parsed only once it is complete.
    """
    end = size
    while end > 0:
        start = _line_start(fd, end - 1)
        try:
            seq = json.loads(os.pread(fd, end - 1 - start, start)).get("seq")
        except (ValueError, AttributeError):
            seq = None
        if isinstance(seq, int):
            return seq
        end = start
    return 0


def _line_start(fd: int, newline: int) -> int:
    """Offset where the line ending at the newline at offset `newline` begins."""
    pos = newline
    while pos > 0:
        block = max(0, pos - TAIL_BYTES)
        found = os.pread(fd, pos - block, block).rfind(b"\n")
        if found >= 0:
            return block + found + 1
        pos = block
    return 0


class HubRegistry:
    """The channel claim and peers, kept by the hub.

    `connection` is the id of this process's WebSocket, set by `HubLink`
    from the hub's pong and cleared when the socket closes. A claim names
    it, so the hub can judge the claim live by that connection's pings.
    """

    def __init__(self, api: HubApi, *, clock: Callable[[], float] = time.monotonic) -> None:
        self.api = api
        self.clock = clock
        self.connection: str | None = None
        self._peers: tuple[float, list[dict[str, Any]]] | None = None

    def claim(self, agent: str, session: str, pid: int) -> bool:
        if self.connection is None:
            return False
        try:
            return self.api.put_session(session, self.connection)
        except HubUnavailableError as exc:
            log.warning("cannot claim the channel: %s", exc)
            return False

    def heartbeat(self, agent: str, session: str) -> bool:
        """Keep the claim, re-pointing it at the current connection after a reconnect.

        While the hub is out of reach this keeps the channel: nobody else
        can claim it either until this connection's pings go stale.
        """
        if self.connection is None:
            return True
        try:
            return self.api.put_session(session, self.connection)
        except HubUnavailableError as exc:
            log.warning("cannot refresh the channel claim: %s", exc)
            return True

    def release(self, agent: str, session: str) -> None:
        try:
            self.api.delete_session(session)
        except BusError as exc:
            log.warning("cannot release the channel: %s", exc)

    def _peer_list(self) -> list[dict[str, Any]]:
        now = self.clock()
        if self._peers is None or now - self._peers[0] > PEERS_TTL_SECONDS:
            self._peers = (now, self.api.peers())
        return self._peers[1]

    def live_agents(self) -> set[str]:
        return {p["name"] for p in self._peer_list() if p.get("live")}

    def teammates(self, agent: str) -> set[str] | None:
        return {p["name"] for p in self._peer_list()}


class HubLink:
    """The WebSocket to the hub: keepalive, the connection id, and sync on push."""

    def __init__(
        self,
        ws_url: str,
        token: str,
        store: HubStore,
        registry: HubRegistry,
        *,
        ping_seconds: float = PING_SECONDS,
        opener: Callable[..., Any] = connect,
    ) -> None:
        self.ws_url = ws_url
        self.token = token
        self.store = store
        self.registry = registry
        self.ping_seconds = ping_seconds
        self.opener = opener
        # Events bind to a running loop, and hooks and the CLI build a link
        # without one, so they are made on first use.
        self._connected: anyio.Event | None = None
        self._want_sync: anyio.Event | None = None

    @property
    def connected(self) -> anyio.Event:
        """Set at the first pong, once the connection id is known."""
        if self._connected is None:
            self._connected = anyio.Event()
        return self._connected

    def _request_sync(self) -> None:
        if self._want_sync is None:
            self._want_sync = anyio.Event()
        self._want_sync.set()

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with self.opener(
                    self.ws_url,
                    subprotocols=[SUBPROTOCOL, self.token],
                    ping_interval=None,
                    open_timeout=15,
                ) as ws:
                    backoff = 1.0
                    await self._session(ws)
            except (OSError, TimeoutError, WebSocketException) as exc:
                log.warning("hub socket: %s: %s", type(exc).__name__, exc)
            finally:
                self.registry.connection = None
            await anyio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

    async def _session(self, ws: Any) -> None:
        async with anyio.create_task_group() as tg:
            tg.start_soon(self._pinger, ws)
            tg.start_soon(self._syncer)
            self._request_sync()
            async for raw in ws:
                try:
                    message = json.loads(raw)
                except ValueError:
                    continue
                if message.get("type") == "pong":
                    if self.registry.connection != message.get("connection"):
                        log.info("hub connection %s", message.get("connection"))
                    self.registry.connection = message.get("connection")
                    self.connected.set()
                elif "seq" in message:
                    self._request_sync()
                elif message.get("type") == "error":
                    log.warning("hub socket error: %s", message.get("error"))
            log.info("hub socket closed: %s", getattr(ws, "close_code", None))
            tg.cancel_scope.cancel()

    async def _pinger(self, ws: Any) -> None:
        while True:
            await ws.send(json.dumps({"action": "ping"}))
            await anyio.sleep(self.ping_seconds)
            self._request_sync()

    async def _syncer(self) -> None:
        """Sync off the socket's task; pushes that arrive meanwhile fold into one more sync."""
        while True:
            if self._want_sync is None:
                self._want_sync = anyio.Event()
            await self._want_sync.wait()
            self._want_sync = anyio.Event()
            try:
                added = await anyio.to_thread.run_sync(self.store.sync)
            except BusError as exc:
                log.warning("mirror sync failed: %s", exc)
                continue
            if added:
                log.info("mirror +%s events", added)
