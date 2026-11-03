"""The hub client against the real `Hub` on moto, joined by an in-process HTTP transport."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import anyio
import httpx
import pytest
from hubfakes import BLOB_HOST, FakeBlobs, blobs, clock, hub, pusher, table, tokens
from websockets.exceptions import ConnectionClosedError

from agent_bus import remote
from agent_bus.bus import Bus, BusError, HubUnavailableError, RateLimitError
from agent_bus.hub import app
from agent_bus.hub.app import Hub
from agent_bus.hub.table import Claim, Table
from agent_bus.models import MessageEvent
from agent_bus.remote import HubApi, HubLink, HubRegistry, HubStore

__all__ = ["blobs", "clock", "hub", "pusher", "table", "tokens"]  # fixtures

URL = "https://hub.test"


def bridge(hub: Hub, *, delay: float = 0.0) -> httpx.MockTransport:
    """API Gateway in one function: the authorizer, then the Lambda's HTTP adapter."""

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.host == BLOB_HOST:
            # The bucket: it must never see the hub token.
            assert "authorization" not in request.headers
            assert isinstance(hub.blobs, FakeBlobs)
            status, body = hub.blobs.serve(
                request.method, request.url.path, request.url.params.get("sig", ""), request.content
            )
            return httpx.Response(status, content=body)
        token = request.headers.get("authorization", "").removeprefix("Bearer ").strip()
        if not token:
            return httpx.Response(401, json={"message": "Unauthorized"})
        agent = hub.table.agent_for_token(token)
        if agent is None:
            return httpx.Response(403, json={"message": "Forbidden"})
        event = {
            "requestContext": {
                "http": {"method": request.method, "path": request.url.path},
                "authorizer": {"lambda": {"agent": agent}},
            },
            "queryStringParameters": dict(request.url.params) or None,
            "body": request.content.decode() or None,
        }
        if delay:
            time.sleep(delay)
        out = app.http(event, hub)
        return httpx.Response(out["statusCode"], headers=out["headers"], content=out["body"])

    return httpx.MockTransport(handle)


Client = Callable[..., tuple[Bus, HubStore, HubRegistry]]


@pytest.fixture
def client(hub: Hub, tokens: dict[str, str], tmp_path: Path) -> Client:
    hub.rate_limit = 1000

    def make(agent: str, *, delay: float = 0.0) -> tuple[Bus, HubStore, HubRegistry]:
        api = HubApi(URL, tokens[agent], transport=bridge(hub, delay=delay))
        store = HubStore(api, remote.hub_home(tmp_path, URL, agent))
        registry = HubRegistry(api)
        return Bus(store, agent, registry=registry), store, registry

    return make


def test_a_sent_message_reaches_the_mirror_only_through_sync(client: Client) -> None:
    alice, alice_store, _ = client("alice")
    _, bob_store, _ = client("bob")
    sent = alice.send("bob", "hello", kind="task")
    assert sent.seq == 1
    assert sent.sender == "alice"
    assert bob_store.read_all().events == []
    assert bob_store.sync() == 1
    assert bob_store.read_all().events == [sent]
    assert bob_store.sync() == 0
    assert alice_store.sync() == 1


def test_task_and_result_round_trip_through_the_hub(client: Client) -> None:
    alice, alice_store, _ = client("alice")
    bob, bob_store, _ = client("bob")
    task = alice.send("bob", "do it", kind="task")
    bob_store.sync()
    assert [m.id for m in bob.inbox()] == [task.id]
    result = bob.send("alice", "done", kind="result", reply_to=task.id)
    assert result.thread == task.thread
    bob_store.sync()
    assert bob.inbox() == []  # the reply acked the task
    alice_store.sync()
    assert [m.id for m in alice.inbox()] == [result.id]
    assert task.id in alice.state().acked


def test_peers_are_teammates_and_count_only_my_unacked(client: Client) -> None:
    alice, alice_store, _ = client("alice")
    dave, _, _ = client("dave")
    alice.send("bob", "one")
    dave.send("bob", "from dave")
    alice_store.sync()
    peers = {p.name: p for p in alice.peers()}
    assert set(peers) == {"bob", "dave"}
    assert peers["bob"].unread_by_them == 1


def test_hub_refusals_reach_the_model_as_bus_errors(client: Client, hub: Hub) -> None:
    alice, _, _ = client("alice")
    with pytest.raises(BusError, match="carol is not on any of your teams"):
        alice.send("carol", "x")
    hub.rate_limit = 1
    alice.send("bob", "one")
    with pytest.raises(RateLimitError, match="await_human"):
        alice.send("bob", "two")


def test_a_revoked_token_is_named_as_such(
    client: Client, table: Table, tokens: dict[str, str]
) -> None:
    alice, _, _ = client("alice")
    table.client.delete_item(
        TableName=table.name,
        Key={"pk": {"S": f"TOKEN#{remote_hash(tokens['alice'])}"}, "sk": {"S": "AGENT"}},
    )
    with pytest.raises(BusError, match="refused this agent's token"):
        alice.send("bob", "x")


def remote_hash(token: str) -> str:
    from agent_bus.hub.table import token_hash

    return token_hash(token)


@pytest.mark.parametrize(
    "respond",
    [
        lambda r: httpx.Response(502, json={"message": "Internal Server Error"}),
        lambda r: httpx.Response(429, json={"message": "Too Many Requests"}),
    ],
)
def test_gateway_failures_are_unavailability(
    respond: Callable[[httpx.Request], httpx.Response], tmp_path: Path
) -> None:
    api = HubApi(URL, "ab_x", transport=httpx.MockTransport(respond))
    with pytest.raises(HubUnavailableError):
        api.peers()


def test_network_errors_are_unavailability() -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    with pytest.raises(HubUnavailableError, match="unreachable"):
        HubApi(URL, "ab_x", transport=httpx.MockTransport(fail)).peers()


# --- the mirror -----------------------------------------------------------------


def test_a_torn_last_line_is_ended_before_new_lines(client: Client) -> None:
    alice, _, _ = client("alice")
    _, bob_store, _ = client("bob")
    first = alice.send("bob", "one")
    bob_store.sync()
    with (bob_store.home / "log.jsonl").open("ab") as fh:
        fh.write(b'{"type": "message", "seq": 9')
    second = alice.send("bob", "two")
    assert bob_store.sync() == 1
    result = bob_store.read_all()
    assert result.events == [first, second]
    assert result.skipped == 1


def test_concurrent_syncs_write_each_event_once(client: Client) -> None:
    alice, _, _ = client("alice")
    sent = [alice.send("bob", f"m{i}").seq for i in range(5)]
    _, bob_store, _ = client("bob", delay=0.05)
    threads = [threading.Thread(target=bob_store.sync) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert [e.seq for e in bob_store.read_all().events] == sent


def test_sync_pages_and_skips_what_it_already_has(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(remote, "PAGE", 2)
    alice, _, _ = client("alice")
    sent = [alice.send("bob", f"m{i}").seq for i in range(5)]
    _, bob_store, _ = client("bob")
    real = bob_store.api.events
    # A hub that repeats the page boundary must not duplicate lines.
    monkeypatch.setattr(bob_store.api, "events", lambda after: real(max(0, after - 1), 2))
    assert bob_store.sync() == 5
    assert [e.seq for e in bob_store.read_all().events] == sent


def test_last_seq_reaches_past_a_long_tail(client: Client, monkeypatch: pytest.MonkeyPatch) -> None:
    alice, _, _ = client("alice")
    _, bob_store, _ = client("bob")
    alice.send("bob", "short")
    alice.send("bob", "x" * 5000)
    bob_store.sync()
    monkeypatch.setattr(remote, "TAIL_BYTES", 1000)
    alice.send("bob", "next")
    assert bob_store.sync() == 1
    assert [e.seq for e in bob_store.read_all().events] == [1, 2, 3]


# --- the registry -------------------------------------------------------------


def test_claims_need_a_connection_and_one_live_session_wins(client: Client, hub: Hub) -> None:
    _, _, first = client("bob")
    _, _, second = client("bob")
    assert not first.claim("bob", "s1", 0)
    hub.connect("bob", "c1", "https://ws")
    hub.connect("bob", "c2", "https://ws")
    first.connection, second.connection = "c1", "c2"
    assert first.claim("bob", "s1", 0)
    assert not second.claim("bob", "s2", 0)
    assert first.heartbeat("bob", "s1")
    first.connection = "c3"  # reconnected; the heartbeat moves the claim to it
    hub.connect("bob", "c3", "https://ws")
    assert first.heartbeat("bob", "s1")
    assert hub.table.claim_of("bob") == Claim("s1", "c3")
    first.release("bob", "s1")
    assert second.claim("bob", "s2", 0)


def test_heartbeat_keeps_the_channel_while_the_hub_is_unreachable() -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down", request=request)

    registry = HubRegistry(HubApi(URL, "ab_x", transport=httpx.MockTransport(fail)))
    registry.connection = "c1"
    assert registry.heartbeat("bob", "s1")
    assert not registry.claim("bob", "s1", 0)


def test_peers_are_fetched_once_per_ttl(client: Client) -> None:
    _, _, registry = client("alice")
    calls: list[int] = []
    real = registry.api.peers

    def counted() -> list[dict[str, Any]]:
        calls.append(1)
        return real()

    registry.api.peers = counted  # type: ignore[method-assign]
    clock = [0.0]
    registry.clock = lambda: clock[0]
    assert registry.teammates("alice") == {"bob", "dave"}
    assert registry.live_agents() == set()
    clock[0] += remote.PEERS_TTL_SECONDS + 1
    registry.live_agents()
    assert len(calls) == 2


# --- the socket ---------------------------------------------------------------


class FakeSocket:
    def __init__(self, frames: list[dict[str, Any]]) -> None:
        self.frames = frames
        self.sent: list[str] = []
        self.close_code = 1001

    async def send(self, data: str) -> None:
        self.sent.append(data)

    def __aiter__(self) -> FakeSocket:
        return self

    async def __anext__(self) -> str:
        if not self.frames:
            await anyio.sleep(0.05)
            raise StopAsyncIteration
        await anyio.sleep(0.01)
        return json.dumps(self.frames.pop(0))


def test_link_learns_its_connection_syncs_on_push_and_reconnects(
    client: Client, hub: Hub, monkeypatch: pytest.MonkeyPatch
) -> None:
    alice, _, _ = client("alice")
    _, bob_store, bob_registry = client("bob")
    alice.send("bob", "waiting since before the socket")
    sockets: list[FakeSocket] = []
    seen: list[str | None] = []
    at_open: list[str | None] = []

    @asynccontextmanager
    async def opener(url: str, **kwargs: Any) -> AsyncIterator[FakeSocket]:
        assert kwargs["subprotocols"] == ["agent-bus", "ab_token"]
        at_open.append(bob_registry.connection)
        frames: list[dict[str, Any]] = [{"type": "pong", "connection": f"c{len(sockets)}"}]
        if not sockets:
            frames.append({"seq": 2})
        ws = FakeSocket(frames)
        sockets.append(ws)
        yield ws
        seen.append(bob_registry.connection)

    monkeypatch.setattr(remote, "MAX_BACKOFF_SECONDS", 0.01)
    link = HubLink("wss://ws", "ab_token", bob_store, bob_registry, opener=opener)

    async def main() -> None:
        with anyio.move_on_after(3):
            async with anyio.create_task_group() as tg:
                tg.start_soon(link.run)
                await link.connected.wait()
                alice.send("bob", "pushed")
                while len(sockets) < 2 or len(bob_store.read_all().events) < 2:
                    await anyio.sleep(0.02)
                tg.cancel_scope.cancel()

    monkeypatch.setattr(anyio, "sleep", _fast_sleep(anyio.sleep))
    anyio.run(main)
    assert seen[0] == "c0"
    # A closed socket's id is never offered in a claim after it closes.
    assert set(at_open) == {None}
    assert json.loads(sockets[0].sent[0]) == {"action": "ping"}
    assert [m.body for m in bob_store.read_all().events if isinstance(m, MessageEvent)] == [
        "waiting since before the socket",
        "pushed",
    ]


class DroppedSocket(FakeSocket):
    """A socket that dies without a close frame (code 1006), on receive or on send."""

    def __init__(self, frames: list[dict[str, Any]], *, on_send: bool) -> None:
        super().__init__(frames)
        self.on_send = on_send

    async def send(self, data: str) -> None:
        if self.on_send:
            raise ConnectionClosedError(None, None)
        await super().send(data)

    async def __anext__(self) -> str:
        if not self.frames:
            await anyio.sleep(0.05)
            raise ConnectionClosedError(None, None)
        return await super().__anext__()


@pytest.mark.parametrize("on_send", [False, True], ids=["on-receive", "on-ping"])
def test_link_reconnects_after_a_socket_drops_without_a_close_frame(
    client: Client, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, on_send: bool
) -> None:
    _, bob_store, bob_registry = client("bob")
    sockets: list[DroppedSocket] = []

    @asynccontextmanager
    async def opener(url: str, **kwargs: Any) -> AsyncIterator[DroppedSocket]:
        ws = DroppedSocket([{"type": "pong", "connection": f"c{len(sockets)}"}], on_send=on_send)
        sockets.append(ws)
        yield ws

    monkeypatch.setattr(remote, "MAX_BACKOFF_SECONDS", 0.01)
    link = HubLink("wss://ws", "ab_token", bob_store, bob_registry, opener=opener)

    async def main() -> None:
        with anyio.move_on_after(3):
            async with anyio.create_task_group() as tg:
                tg.start_soon(link.run)
                while len(sockets) < 3:
                    await anyio.sleep(0.02)
                tg.cancel_scope.cancel()

    monkeypatch.setattr(anyio, "sleep", _fast_sleep(anyio.sleep))
    anyio.run(main)
    assert len(sockets) >= 3
    assert "hub socket: ConnectionClosedError" in caplog.text


def test_the_server_starts_a_failed_link_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from agent_bus import server as server_module

    remote.write_token(remote.hub_home(tmp_path / "bus", URL, "bob") / remote.TOKEN_NAME, "ab_x")
    server = _hub_server(tmp_path)
    server.link.ws_url = "wss://ws"
    runs: list[int] = []

    async def run() -> None:
        runs.append(1)
        if len(runs) == 1:
            raise ValueError("not a link error")
        await anyio.sleep_forever()

    monkeypatch.setattr(server.link, "run", run)
    monkeypatch.setattr(server_module, "HELLO_RETRY_SECONDS", 0.01)

    async def main() -> None:
        with anyio.move_on_after(1):
            async with anyio.create_task_group() as tg:
                tg.start_soon(server.keep_link)
                while len(runs) < 2:
                    await anyio.sleep(0.01)
                tg.cancel_scope.cancel()

    anyio.run(main)
    assert len(runs) == 2
    assert "hub link failed" in caplog.text
    assert "ValueError: not a link error" in caplog.text


def test_a_failed_background_task_stops_the_server_at_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from agent_bus import server as server_module

    server = _hub_server(tmp_path)
    released: list[bool] = []
    monkeypatch.setattr(server, "release", lambda: released.append(True))

    def exit_now(code: int) -> None:
        raise SystemExit(code)

    monkeypatch.setattr("agent_bus.server.os._exit", exit_now)

    async def broken(arg: str) -> None:
        raise RuntimeError(f"broken {arg}")

    with pytest.raises(SystemExit) as stopped:
        anyio.run(server_module.exit_on_error, server, broken, "loop")
    assert stopped.value.code == 1
    assert released == [True]
    assert "broken failed; stopping the server" in caplog.text
    assert "RuntimeError: broken loop" in caplog.text


def _fast_sleep(real: Callable[[float], Any]) -> Callable[[float], Any]:
    """Backoff and ping intervals shrink so the test runs in well under a second."""

    async def sleep(delay: float) -> None:
        await real(min(delay, 0.05))

    return sleep


# --- the server in hub mode ---------------------------------------------------


def _hub_server(tmp_path: Path, agent: str = "bob") -> Any:
    from agent_bus.config import load
    from agent_bus.server import BusServer

    cfg = load(
        {"AGENT_BUS_SESSION": "1", "AGENT_BUS_NAME": agent, "AGENT_BUS_HUB": URL},
        bus_dir=str(tmp_path / "bus"),
    )
    return BusServer(cfg)


def test_a_server_without_a_token_stays_off_the_bus_and_says_why(tmp_path: Path) -> None:
    server = _hub_server(tmp_path)
    assert server.bus is None
    assert server.list_tools() == []
    assert "no hub token at" in server.instructions()


def test_a_server_whose_token_is_another_agents_stays_off_the_bus(tmp_path: Path) -> None:
    remote.write_token(remote.hub_home(tmp_path / "bus", URL, "bob") / remote.TOKEN_NAME, "ab_x")
    server = _hub_server(tmp_path)
    assert server.bus is not None
    server.backend.api.hello = lambda: {"agent": "mallory", "ws": "wss://ws"}
    anyio.run(server.check_hub)
    assert server.bus is None
    assert server.link is None
    assert "belongs to agent 'mallory', not 'bob'" in server.instructions()


def test_a_server_learns_the_socket_address_from_hello(tmp_path: Path) -> None:
    remote.write_token(remote.hub_home(tmp_path / "bus", URL, "bob") / remote.TOKEN_NAME, "ab_x")
    server = _hub_server(tmp_path)
    server.backend.api.hello = lambda: {"agent": "bob", "ws": "wss://ws/bus"}
    anyio.run(server.check_hub)
    assert server.bus is not None
    assert server.link.ws_url == "wss://ws/bus"


def test_a_token_readable_by_others_is_refused(tmp_path: Path) -> None:
    path = remote.hub_home(tmp_path / "bus", URL, "bob") / remote.TOKEN_NAME
    remote.write_token(path, "ab_x")
    path.chmod(0o644)
    server = _hub_server(tmp_path)
    assert "readable by others" in server.instructions()


def test_an_attachment_round_trips_through_the_bucket(
    client: Client, tmp_path: Path, blobs: FakeBlobs
) -> None:
    alice, _, _ = client("alice")
    bob, bob_store, _ = client("bob")
    source = tmp_path / "review.py"
    source.write_text("def f():\n    return 1\n")
    sent = alice.send("bob", "please review", kind="task", attachments=[source])
    bob_store.sync()
    [(attachment, path)] = bob.attachments(sent.id)
    assert path.read_bytes() == source.read_bytes()
    assert path.parent == bob_store.home / "attachments"
    assert attachment.name == "review.py"

    # A copy that rots on disk is fetched again; a bucket object that changed is refused.
    path.write_text("tampered")
    assert bob.attachments(sent.id)[0][1].read_bytes() == source.read_bytes()
    path.unlink()
    blobs.objects[f"alice/{attachment.sha256}"] = b"swapped"
    with pytest.raises(ValueError, match="another sha256"):
        bob.attachments(sent.id)
    assert not path.exists()


def test_peers_carry_the_hubs_status_and_activity(client: Client, hub: Hub) -> None:
    alice, _, _ = client("alice")
    bob, _, _ = client("bob")
    task = alice.send("bob", "do it", kind="task")
    bob.report_activity("s-bob", "working")
    peers = {p.name: p for p in alice.peers()}
    assert [t.msg_id for t in peers["bob"].status.tasks] == [task.id]
    assert peers["bob"].status.state == "offline"  # bob holds no channel
    assert hub.table.status_of("bob")[0] is not None
