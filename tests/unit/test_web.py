from __future__ import annotations

import http.client
import json
import re
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from agent_bus import config, web
from agent_bus.bus import Bus
from agent_bus.remote import HubStore
from agent_bus.store import Store
from agent_bus.web import READ_SHOWN, WebServer, make_server


class Client:
    def __init__(self, server: WebServer) -> None:
        self.server = server
        self.port = server.server_address[1]

    def request(
        self,
        method: str,
        path: str,
        payload: Any = None,
        *,
        csrf: str | None = "",
        host: str | None = None,
        raw: bytes | None = None,
        content_length: int | None = None,
    ) -> tuple[int, http.client.HTTPMessage, bytes]:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.putrequest(method, path, skip_host=True)
            conn.putheader("Host", host or f"127.0.0.1:{self.port}")
            token = self.server.csrf if csrf == "" else csrf
            if token is not None:
                conn.putheader("X-CSRF-Token", token)
            body = raw if raw is not None else b""
            if raw is None and payload is not None:
                body = json.dumps(payload).encode()
            if method == "POST":
                conn.putheader("Content-Type", "application/json")
                length = content_length if content_length is not None else len(body)
                conn.putheader("Content-Length", str(length))
            conn.endheaders(body if method == "POST" else None)
            resp = conn.getresponse()
            return resp.status, resp.msg, resp.read()
        finally:
            conn.close()

    def get_json(self, path: str) -> Any:
        status, _, data = self.request("GET", path)
        assert status == 200, data
        return json.loads(data)

    def post(self, path: str, payload: Any, **kw: Any) -> tuple[int, Any]:
        status, _, data = self.request("POST", path, payload, **kw)
        return status, json.loads(data) if data else None


@pytest.fixture
def owner_store(tmp_path: Path) -> Store:
    return Store(tmp_path / "bus")


@pytest.fixture
def owner(owner_store: Store) -> Bus:
    return Bus(owner_store, "owner", rate_limit=1000)


@pytest.fixture
def personal(owner_store: Store) -> Bus:
    return Bus(owner_store, "personal", rate_limit=1000)


ServerFactory = Callable[..., Client]


@pytest.fixture
def serve(owner: Bus) -> Iterator[ServerFactory]:
    started: list[tuple[WebServer, threading.Thread]] = []

    def start(**kwargs: Any) -> Client:
        server = make_server(owner, port=0, **kwargs)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        started.append((server, thread))
        return Client(server)

    yield start
    for server, thread in started:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def client(serve: ServerFactory) -> Client:
    return serve()


def by_id(messages: list[dict[str, Any]], msg_id: str) -> dict[str, Any]:
    return next(m for m in messages if m["id"] == msg_id)


def test_page_has_csrf_meta_and_no_inline_script(client: Client) -> None:
    status, headers, data = client.request("GET", "/")
    assert status == 200
    html = data.decode()
    meta = re.search(r'<meta name="csrf" content="([^"]+)"', html)
    assert meta is not None
    assert meta.group(1) == client.server.csrf
    assert '<script src="/app.js"' in html
    for body in re.findall(r"<script\b[^>]*>(.*?)</script>", html, re.S):
        assert body.strip() == ""
    assert headers["Content-Security-Policy"] == "default-src 'self'"


def test_security_headers_on_every_response(client: Client) -> None:
    for path in ("/", "/app.js", "/app.css", "/api/inbox"):
        status, headers, _ = client.request("GET", path)
        assert status == 200, path
        assert headers["Content-Security-Policy"] == "default-src 'self'"
        assert headers["X-Content-Type-Options"] == "nosniff"
        assert headers["Cache-Control"] == "no-store"
        assert headers["Referrer-Policy"] == "no-referrer"


def test_static_assets_never_render_with_innerhtml_or_embed_the_token(client: Client) -> None:
    _, _, js = client.request("GET", "/app.js")
    assert b"innerHTML" not in js
    assert client.server.csrf.encode() not in js


def test_inbox_orders_unread_first_then_read_newest_first(
    client: Client, owner: Bus, personal: Bus
) -> None:
    read = [personal.send("owner", f"read {i}") for i in range(READ_SHOWN + 3)]
    for msg in read:
        owner.ack(msg.id)
    unread = [personal.send("owner", f"unread {i}") for i in range(3)]

    data = client.get_json("/api/inbox")
    assert data["me"] == "owner"
    assert data["peer"] == "personal"
    ids = [m["id"] for m in data["messages"]]
    assert ids[:3] == [m.id for m in reversed(unread)]
    assert ids[3:] == [m.id for m in reversed(read)][:READ_SHOWN]
    assert all(m["unread"] for m in data["messages"][:3])
    assert not any(m["unread"] for m in data["messages"][3:])

    first = data["messages"][0]
    assert first["from"] == "personal"
    assert first["body"] == "unread 2"
    assert first["attachments"] == []
    assert first["replies"] == []
    assert re.match(r"\d{4}-\d{2}-\d{2}T", first["time"])


def test_inbox_answerable_and_reply_kinds(client: Client, personal: Bus) -> None:
    task = personal.send("owner", "do it", kind="task")
    text = personal.send("owner", "fyi", kind="text")
    result = personal.send("owner", "done", kind="result")
    waiting = personal.send("owner", "need a human", kind="await_human")

    messages = client.get_json("/api/inbox")["messages"]
    t = by_id(messages, task.id)
    assert (t["answerable"], t["reply_kinds"], t["kind"]) == (True, ["text", "result"], "task")
    x = by_id(messages, text.id)
    assert (x["answerable"], x["reply_kinds"]) == (True, ["text"])
    for msg in (result, waiting):
        m = by_id(messages, msg.id)
        assert m["answerable"] is False
        assert "text" not in m["reply_kinds"]


def test_inbox_lists_the_owners_earlier_reply_under_its_message(
    client: Client, owner: Bus, personal: Bus
) -> None:
    question = personal.send("owner", "question?")
    answer = owner.send("personal", "my answer", reply_to=question.id)
    other = personal.send("owner", "unrelated")

    messages = client.get_json("/api/inbox")["messages"]
    shown = by_id(messages, question.id)
    assert [r["id"] for r in shown["replies"]] == [answer.id]
    assert shown["replies"][0]["body"] == "my answer"
    assert shown["replies"][0]["kind"] == "text"
    assert shown["replies"][0]["time"]
    assert by_id(messages, other.id)["replies"] == []


def test_reply_goes_to_the_original_sender_with_reply_to(client: Client, personal: Bus) -> None:
    task = personal.send("owner", "do it", kind="task")
    status, data = client.post("/api/reply", {"msg_id": task.id, "kind": "result", "body": "done"})
    assert status == 200
    inbox = personal.inbox()
    assert [m.id for m in inbox] == [data["id"]]
    assert inbox[0].reply_to == task.id
    assert inbox[0].kind == "result"
    assert inbox[0].sender == "owner"
    assert inbox[0].thread == task.thread


def test_reply_acks_the_original(client: Client, owner: Bus, personal: Bus) -> None:
    msg = personal.send("owner", "hi")
    status, _ = client.post("/api/reply", {"msg_id": msg.id, "kind": "text", "body": "hello"})
    assert status == 200
    assert owner.inbox() == []


def test_result_reply_to_a_non_task_is_refused(client: Client, personal: Bus) -> None:
    msg = personal.send("owner", "just text")
    status, data = client.post("/api/reply", {"msg_id": msg.id, "kind": "result", "body": "x"})
    assert status == 400
    assert data["error"]
    assert personal.inbox() == []


@pytest.mark.parametrize("kind", ["result", "await_human"])
def test_any_reply_to_a_chain_ending_message_is_refused(
    client: Client, personal: Bus, kind: str
) -> None:
    msg = personal.send("owner", "end of chain", kind=kind)  # type: ignore[arg-type]
    for reply_kind in ("text", "result", "task"):
        status, data = client.post(
            "/api/reply", {"msg_id": msg.id, "kind": reply_kind, "body": "more talk"}
        )
        assert status == 400, reply_kind
        assert "error" in data
    assert personal.inbox() == []


def test_reply_to_an_unknown_message_is_refused(client: Client) -> None:
    status, data = client.post("/api/reply", {"msg_id": "nope", "kind": "text", "body": "x"})
    assert status == 400
    assert data["error"]


def test_a_body_with_a_secret_is_refused_with_the_leak_scan_message(
    client: Client, personal: Bus
) -> None:
    secret = "gh" + "p_" + "A" * 36
    status, data = client.post("/api/send", {"body": f"here: {secret}"})
    assert status == 400
    assert "GitHub token" in data["error"]
    assert secret not in data["error"]
    assert personal.inbox() == []

    msg = personal.send("owner", "q")
    status, data = client.post(
        "/api/reply", {"msg_id": msg.id, "kind": "text", "body": f"here: {secret}"}
    )
    assert status == 400
    assert secret not in data["error"]
    assert personal.inbox() == []


def test_send_reaches_the_default_peer(client: Client, personal: Bus) -> None:
    status, data = client.post("/api/send", {"body": "hello personal"})
    assert status == 200
    inbox = personal.inbox()
    assert [m.id for m in inbox] == [data["id"]]
    assert inbox[0].body == "hello personal"
    assert inbox[0].kind == "text"
    assert inbox[0].reply_to is None


def test_send_reaches_a_custom_peer(serve: ServerFactory, owner_store: Store) -> None:
    other = Bus(owner_store, "legion", rate_limit=1000)
    client = serve(peer="legion")
    assert client.get_json("/api/inbox")["peer"] == "legion"
    status, _ = client.post("/api/send", {"body": "hi legion"})
    assert status == 200
    assert [m.body for m in other.inbox()] == ["hi legion"]


def test_send_with_an_empty_body_is_refused(client: Client) -> None:
    status, data = client.post("/api/send", {"body": "   "})
    assert status == 400
    assert data["error"]


def test_ack_marks_messages_read(client: Client, owner: Bus, personal: Bus) -> None:
    a = personal.send("owner", "a")
    b = personal.send("owner", "b")
    status, data = client.post("/api/ack", {"msg_ids": [a.id, b.id]})
    assert status == 200
    assert sorted(data["acked"]) == sorted([a.id, b.id])
    assert owner.inbox() == []
    messages = client.get_json("/api/inbox")["messages"]
    assert not any(m["unread"] for m in messages)
    assert {m["id"] for m in messages} == {a.id, b.id}


def test_ack_of_an_unknown_id_is_a_400(client: Client) -> None:
    status, data = client.post("/api/ack", {"msg_ids": ["nope"]})
    assert status == 400
    assert data["error"]


def test_loading_the_inbox_does_not_ack(client: Client, owner: Bus, personal: Bus) -> None:
    msg = personal.send("owner", "still unread")
    client.get_json("/api/inbox")
    client.get_json("/api/inbox")
    assert [m.id for m in owner.inbox()] == [msg.id]


@pytest.mark.parametrize("csrf", [None, "wrong-token"])
@pytest.mark.parametrize(
    ("path", "payload"),
    [
        ("/api/send", {"body": "x"}),
        ("/api/reply", {"msg_id": "x", "kind": "text", "body": "x"}),
        ("/api/ack", {"msg_ids": []}),
    ],
)
def test_post_without_the_csrf_token_is_forbidden(
    client: Client, personal: Bus, path: str, payload: Any, csrf: str | None
) -> None:
    status, _, _ = client.request("POST", path, payload, csrf=csrf)
    assert status == 403
    assert personal.inbox() == []


def test_wrong_host_header_is_forbidden(client: Client) -> None:
    for host in ("evil.example", f"evil.example:{client.port}", "127.0.0.1:1", "127.0.0.1"):
        for path in ("/", "/api/inbox", "/app.js"):
            status, _, _ = client.request("GET", path, host=host)
            assert status == 403, (host, path)
        status, _, _ = client.request("POST", "/api/send", {"body": "x"}, host=host)
        assert status == 403, host


def test_localhost_host_header_is_accepted(client: Client) -> None:
    status, _, _ = client.request("GET", "/api/inbox", host=f"localhost:{client.port}")
    assert status == 200


def test_oversize_body_is_413(client: Client, personal: Bus) -> None:
    status, _, _ = client.request(
        "POST", "/api/send", raw=b'{"body": "x"}', content_length=1024 * 1024 + 1
    )
    assert status == 413
    assert personal.inbox() == []


def test_non_json_body_is_a_client_error(client: Client) -> None:
    status, _, _ = client.request("POST", "/api/send", raw=b"not json")
    assert 400 <= status < 500


def test_make_server_refuses_a_non_loopback_host(owner: Bus) -> None:
    for host in ("0.0.0.0", "192.168.1.5", ""):
        with pytest.raises(ValueError, match=r"."):
            make_server(owner, host=host, port=0)


def test_make_server_binds_loopback_on_a_free_port(owner: Bus) -> None:
    server = make_server(owner, port=0)
    try:
        host, port = server.server_address[:2]
        assert host == "127.0.0.1"
        assert port != 0
        assert len(server.csrf) >= 32
        assert server.peer == "personal"
    finally:
        server.server_close()


def test_each_server_has_its_own_csrf_token(owner: Bus) -> None:
    a = make_server(owner, port=0)
    b = make_server(owner, port=0)
    try:
        assert a.csrf != b.csrf
    finally:
        a.server_close()
        b.server_close()


def test_a_failing_sync_is_a_503_with_only_the_class_name(serve: ServerFactory) -> None:
    class HubDownError(Exception):
        pass

    def boom() -> None:
        raise HubDownError("secret detail https://hub.example/token")

    client = serve(sync=boom)
    status, _, data = client.request("GET", "/api/inbox")
    assert status == 503
    assert json.loads(data) == {"error": "HubDownError"}
    assert b"secret" not in data


def test_sync_runs_on_each_inbox_read(serve: ServerFactory, personal: Bus) -> None:
    calls: list[int] = []
    client = serve(sync=lambda: calls.append(1))
    client.get_json("/api/inbox")
    assert len(calls) == 1
    client.get_json("/api/inbox")
    assert len(calls) == 2
    client.request("GET", "/")
    client.request("GET", "/app.js")
    assert len(calls) == 2


def test_sync_runs_before_the_inbox_is_read(
    serve: ServerFactory, owner_store: Store, personal: Bus
) -> None:
    # A message that "arrives" during sync (as a hub pull would deliver it) is in the answer.
    def deliver() -> None:
        personal.send("owner", "arrived in sync")

    client = serve(sync=deliver)
    messages = client.get_json("/api/inbox")["messages"]
    assert [m["body"] for m in messages] == ["arrived in sync"]


def test_requests_are_not_logged(
    client: Client, personal: Bus, capsys: pytest.CaptureFixture[str]
) -> None:
    client.post("/api/send", {"body": "private words"})
    client.get_json("/api/inbox")
    captured = capsys.readouterr()
    assert "private words" not in captured.err + captured.out
    assert "/api/" not in captured.err + captured.out


def test_inbox_backend_is_local_without_a_hub(client: Client) -> None:
    assert client.get_json("/api/inbox")["backend"] == {"mode": "local", "hub": None}


def test_inbox_backend_is_hub_with_the_url(serve: ServerFactory) -> None:
    client = serve(hub="https://hub.example/x")
    backend = client.get_json("/api/inbox")["backend"]
    assert backend == {"mode": "hub", "hub": "https://hub.example/x"}


def test_app_js_has_the_local_banner_and_no_innerhtml(client: Client) -> None:
    _, _, js = client.request("GET", "/app.js")
    text = js.decode()
    assert (
        '"LOCAL bus — not connected to a hub. Messages stay on this machine. "'
        ' + "Set AGENT_BUS_HUB (or the plugin\'s hub_url) and restart."'
    ) in " ".join(text.split())
    assert "banner-local" in text
    assert "innerHTML" not in text
    _, _, css = client.request("GET", "/app.css")
    assert b".banner-local" in css


def test_app_js_has_the_answer_controls_and_no_kind_select(client: Client) -> None:
    _, _, js = client.request("GET", "/app.js")
    text = js.decode()
    for label in ("Да", "Нет", "Ответить", "Отправить"):
        assert f"'{label}'" in text
    assert "el('select'" not in text
    assert "<select" not in text
    assert "innerHTML" not in text


@pytest.mark.parametrize("answer", ["да", "нет"])
def test_a_task_answered_with_yes_or_no_arrives_as_a_result(
    client: Client, personal: Bus, answer: str
) -> None:
    task = personal.send("owner", "ok?", kind="task")
    status, _ = client.post("/api/reply", {"msg_id": task.id, "kind": "result", "body": answer})
    assert status == 200
    (got,) = personal.inbox()
    assert (got.kind, got.body, got.reply_to) == ("result", answer, task.id)


@pytest.mark.parametrize("original_kind", ["task", "text"])
def test_a_remark_is_a_text_reply(client: Client, personal: Bus, original_kind: str) -> None:
    msg = personal.send("owner", "hello", kind=original_kind)  # type: ignore[arg-type]
    status, _ = client.post("/api/reply", {"msg_id": msg.id, "kind": "text", "body": "a remark"})
    assert status == 200
    (got,) = personal.inbox()
    assert (got.kind, got.body, got.reply_to) == ("text", "a remark", msg.id)


def test_result_to_a_text_is_a_400(client: Client, personal: Bus) -> None:
    msg = personal.send("owner", "fyi", kind="text")
    status, _ = client.post("/api/reply", {"msg_id": msg.id, "kind": "result", "body": "да"})
    assert status == 400
    assert personal.inbox() == []


@pytest.mark.parametrize("kind", ["result", "await_human"])
def test_result_and_await_human_have_no_reply_controls(
    client: Client, personal: Bus, kind: str
) -> None:
    msg = personal.send("owner", "end", kind=kind)  # type: ignore[arg-type]
    shown = by_id(client.get_json("/api/inbox")["messages"], msg.id)
    assert shown["answerable"] is False
    assert shown["reply_kinds"] == []


class _FakeHubStore(HubStore):
    def __init__(self) -> None:
        pass


class _FakeBus:
    me = "owner"


def _run_without_serving(
    monkeypatch: pytest.MonkeyPatch, store: object, cfg: config.Config
) -> None:
    backend = SimpleNamespace(store=store, registry=None)
    monkeypatch.setattr(web, "open_backend", lambda _cfg: backend)
    monkeypatch.setattr(web, "Bus", lambda *a, **k: _FakeBus())
    monkeypatch.setattr(WebServer, "serve_forever", lambda self, *a, **k: None)
    assert web.run(cfg, port=0) == 0


def test_run_start_line_in_local_mode(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    cfg = config.load({}, bus_dir=str(tmp_path), agent="owner")
    _run_without_serving(monkeypatch, Store(tmp_path / "bus"), cfg)
    captured = capsys.readouterr()
    assert re.fullmatch(
        r"agent-bus web: http://127\.0\.0\.1:\d+/ as owner — "
        r"LOCAL bus \(not connected to a hub; set AGENT_BUS_HUB\)\n",
        captured.out,
    )
    assert captured.err.startswith("WARNING:")


def test_run_start_line_in_hub_mode(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    cfg = config.load({}, bus_dir=str(tmp_path), agent="owner", hub="https://hub.example/x")
    _run_without_serving(monkeypatch, _FakeHubStore(), cfg)
    captured = capsys.readouterr()
    assert re.fullmatch(
        r"agent-bus web: http://127\.0\.0\.1:\d+/ as owner — hub https://hub\.example/x\n",
        captured.out,
    )
    assert "WARNING" not in captured.err
