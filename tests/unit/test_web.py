from __future__ import annotations

import http.client
import json
import re
import threading
import time
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


def test_app_js_is_the_messenger_page(client: Client) -> None:
    _, _, js = client.request("GET", "/app.js")
    text = js.decode()
    for label in (
        "ждёт ответа",
        "новое",
        "отвечено",
        "Да",
        "Нет",
        "Ответить",
        "Отправить",
        "Вы ответили",
        "Написать",
    ):
        assert label in text, label
    for route in ("/api/threads", "/api/thread", "/api/say", "/api/reply", "/api/send"):
        assert route in text, route
    # the page no longer lists the inbox or acks in bulk
    assert "Ack shown" not in text
    assert "/api/inbox" not in text
    assert "/api/ack" not in text
    assert "innerHTML" not in text
    assert "el('select'" not in text
    assert "<select" not in text


def test_app_js_labels_the_new_message_area_with_the_peer_name(client: Client) -> None:
    _, _, js = client.request("GET", "/app.js")
    text = js.decode()
    # the label is built from the peer name the server sends, not a fixed word
    assert re.search(r"Написать\s*['\"`]?\s*\+\s*[\w.]*peer\b", text)
    assert "Написать personal" not in text


def _js_function(text: str, name: str) -> str:
    """Body of `const <name> = ...;` up to the next top-level const."""
    start = text.index(f"const {name} = ")
    end = text.index("\n  const ", start + 1)
    return text[start:end]


def test_app_js_poll_refreshes_the_open_thread_only(client: Client) -> None:
    _, _, js = client.request("GET", "/app.js")
    text = js.decode()
    poll = _js_function(text, "poll")
    assert "setInterval(poll, 10000)" in text
    assert "setInterval(load" not in text
    # the thread call is guarded by the selected thread and never opens one
    assert "const thread = state.selected;" in poll
    assert "if (!thread || state.selected !== thread) return;" in poll
    assert poll.count("'/api/thread'") == 1
    assert poll.index("await load()") < poll.index("'/api/thread'")
    assert "openThread" not in poll
    assert "state.selected =" not in poll
    # a failed thread call shows the error and keeps the old view
    assert "state.error = String(e.message" in poll
    catch = poll[poll.index("catch (e)") :]
    assert "state.view =" not in catch
    # the radio choice is dropped only when the task is no longer open
    assert "if (!view.open_task) state.choice = '';" in poll


def test_app_js_render_keeps_scroll_drafts_and_focus_across_a_refresh(client: Client) -> None:
    _, _, js = client.request("GET", "/app.js")
    text = js.decode()
    render = _js_function(text, "render")
    assert "scrollTop" in render
    assert "scrollHeight - msgs.scrollTop - msgs.clientHeight" in render
    assert "nearBottom" in render
    assert "window.scrollTo(0, pageTop)" in render
    assert "selectionStart" in render
    assert "selectionEnd" in render
    assert "setSelectionRange(focus.start, focus.end)" in render
    assert "area.focus(" in render
    # the drafts are state, re-applied on every rebuild
    assert "area.value = state.say;" in text
    assert "area.value = state.newBody;" in text
    assert "radio.checked = state.choice === value;" in text
    # nothing skips a refresh because a textarea has focus any more
    assert "typing" not in text


def test_app_js_a_radio_choice_never_carries_to_a_newer_open_task(client: Client) -> None:
    # A poll can make a newer task the open one; a Да chosen for the older task must not
    # stay checked for it, or one click answers a task the owner has not read.
    _, _, js = client.request("GET", "/app.js")
    box = _js_function(js.decode(), "answerBox")
    reset = "if (state.choiceFor !== taskId) { state.choice = ''; state.choiceFor = taskId; }"
    assert reset in box
    assert box.index(reset) < box.index("radio.checked = state.choice === value;")
    assert "state.choiceFor = taskId;" in box.split("addEventListener('change'", 1)[1]


def test_a_message_after_the_thread_was_opened_is_acked_by_the_next_thread_call(
    client: Client, owner: Bus, personal: Bus
) -> None:
    first = personal.send("owner", "first")
    open_thread(client, first.thread)
    later = personal.send("owner", "later", reply_to=first.id)

    # the poll's list call acks nothing and shows the new message as unread
    assert thread_row(client, first.thread)["unread"] == 1
    assert [m.id for m in owner.inbox()] == [later.id]

    view = open_thread(client, first.thread)
    assert [m["body"] for m in view["messages"]] == ["first", "later"]
    assert owner.inbox() == []
    assert thread_row(client, first.thread)["unread"] == 0


def test_app_css_stacks_the_panes_on_narrow_screens(client: Client) -> None:
    _, _, css = client.request("GET", "/app.css")
    assert b"@media (max-width: 800px)" in css


def _css_rule(css: str, selector: str) -> str:
    match = re.search(r"(?m)^" + re.escape(selector) + r" \{([^}]*)\}", css)
    assert match, selector
    return " ".join(match.group(1).split())


def test_app_css_two_pane_layout_fills_the_window(client: Client) -> None:
    _, _, raw = client.request("GET", "/app.css")
    css = raw.decode()
    # the list never grows with its content: fixed basis and a zero min-width
    lst = _css_rule(css, ".list")
    assert "flex: 0 0 340px" in lst
    assert "min-width: 0" in lst
    assert "overflow-y: auto" in lst
    detail = _css_rule(css, ".detail")
    assert "flex: 1 1 0" in detail
    assert "min-width: 0" in detail
    assert "max-width: 70rem" not in css
    # a row title is one line with an ellipsis
    title = _css_rule(css, ".thread .title")
    for part in ("white-space: nowrap", "overflow: hidden", "text-overflow: ellipsis"):
        assert part in title
    # #msgs is the scroller between the header and the reply area
    msgs = _css_rule(css, ".msgs")
    assert "overflow-y: auto" in msgs
    assert "flex: 1 1 auto" in msgs
    # below 800px the panes stack and the list scrolls within 30vh
    narrow = css[css.index("@media (max-width: 800px)") :]
    assert "flex-direction: column" in narrow
    assert "max-height: 30vh" in narrow
    # the page itself never scrolls, at any width: only the panes do
    assert "html, body { height: 100%; overflow: hidden; }" in css
    assert "overflow: auto" not in narrow
    assert "height: auto" not in narrow


def test_app_js_reply_area_follows_msgs_inside_the_thread_pane(client: Client) -> None:
    _, _, js = client.request("GET", "/app.js")
    detail = _js_function(js.decode(), "detail")
    assert detail.index("id: 'msgs'") < detail.index("class: 'reply'")
    assert detail.index("pane.append(list)") < detail.index("pane.append(reply)")
    assert "pane.append(reply)" in detail
    assert "reply.append(el('div', { class: 'row' }, area)" in detail
    assert "innerHTML" not in detail


# --- messenger layout: /api/threads, /api/thread, /api/say ---------------------------------


def threads_of(client: Client) -> list[dict[str, Any]]:
    data = client.get_json("/api/threads")
    assert isinstance(data["threads"], list)
    return list(data["threads"])


def thread_row(client: Client, thread: str) -> dict[str, Any]:
    return next(t for t in threads_of(client) if t["thread"] == thread)


def open_thread(client: Client, thread: str) -> dict[str, Any]:
    status, view = client.post("/api/thread", {"thread": thread})
    assert status == 200, view
    assert isinstance(view, dict)
    return view


def test_threads_envelope_has_me_peer_and_backend(client: Client, serve: ServerFactory) -> None:
    data = client.get_json("/api/threads")
    assert data["me"] == "owner"
    assert data["peer"] == "personal"
    assert data["backend"] == {"mode": "local", "hub": None}
    assert data["threads"] == []
    hub = serve(hub="https://hub.example/x")
    assert hub.get_json("/api/threads")["backend"] == {
        "mode": "hub",
        "hub": "https://hub.example/x",
    }


def test_threads_list_only_threads_involving_me(
    client: Client, owner_store: Store, owner: Bus, personal: Bus
) -> None:
    legion = Bus(owner_store, "legion", rate_limit=1000)
    mine_in = personal.send("owner", "to me")
    mine_out = owner.send("legion", "from me")
    foreign = legion.send("personal", "not about the owner")
    legion.send("personal", "still not", reply_to=foreign.id)

    ids = [t["thread"] for t in threads_of(client)]
    assert sorted(ids) == sorted([mine_in.thread, mine_out.thread])
    assert foreign.thread not in ids


def test_threads_are_sorted_newest_activity_first(
    client: Client, owner: Bus, personal: Bus
) -> None:
    a = personal.send("owner", "thread a")
    time.sleep(0.01)
    b = personal.send("owner", "thread b")
    time.sleep(0.01)
    c = personal.send("owner", "thread c")
    assert [t["thread"] for t in threads_of(client)] == [c.thread, b.thread, a.thread]

    time.sleep(0.01)
    personal.send("owner", "a again", reply_to=a.id)
    rows = threads_of(client)
    assert [t["thread"] for t in rows] == [a.thread, c.thread, b.thread]
    times = [t["last_time"] for t in rows]
    assert times == sorted(times, reverse=True)
    assert re.match(r"\d{4}-\d{2}-\d{2}T", rows[0]["last_time"])


def test_last_time_is_the_newest_message_of_the_thread(
    client: Client, owner: Bus, personal: Bus
) -> None:
    first = personal.send("owner", "q")
    time.sleep(0.01)
    owner.send("personal", "a", reply_to=first.id)
    row = thread_row(client, first.thread)
    view = open_thread(client, first.thread)
    assert row["last_time"] == view["messages"][-1]["time"]
    assert row["last_time"] != view["messages"][0]["time"]


def test_title_is_the_stripped_first_line_of_the_opening_message(
    client: Client, owner: Bus, personal: Bus
) -> None:
    opening = personal.send("owner", "  Deploy plan  \nsecond line\nthird")
    owner.send("personal", "a later reply is not the title", reply_to=opening.id)
    assert thread_row(client, opening.thread)["title"] == "Deploy plan"


def test_title_is_at_most_120_characters(client: Client, personal: Bus) -> None:
    opening = personal.send("owner", "x" * 300)
    title = thread_row(client, opening.thread)["title"]
    assert 0 < len(title) <= 120
    assert set(title) <= {"x"}


def test_counterpart_of_an_incoming_and_of_my_own_opening(
    client: Client, owner: Bus, personal: Bus, owner_store: Store
) -> None:
    legion = Bus(owner_store, "legion", rate_limit=1000)
    incoming = personal.send("owner", "from personal")
    outgoing = owner.send("legion", "to legion")
    legion.send("owner", "legion answers", reply_to=outgoing.id)
    # a third party joins the incoming thread later: the counterpart stays the opener's other side
    owner.send("personal", "my answer", reply_to=incoming.id)

    assert thread_row(client, incoming.thread)["counterpart"] == "personal"
    assert thread_row(client, outgoing.thread)["counterpart"] == "legion"


def test_pure_helpers_thread_counterpart_and_status(owner: Bus, personal: Bus) -> None:
    opening = owner.send("personal", "mine first")
    personal.send("owner", "task", kind="task", reply_to=opening.id)
    messages = owner.history(thread=opening.thread, last=None)
    assert web.thread_counterpart(messages, "owner") == "personal"
    assert web.thread_counterpart(messages, "personal") == "owner"
    assert web.thread_status(messages, "owner", set()) == "waiting"
    assert web.answered_result(messages, "owner", messages[1].id) is None
    result = owner.send("personal", "да", kind="result", reply_to=messages[1].id)
    messages = owner.history(thread=opening.thread, last=None)
    found = web.answered_result(messages, "owner", messages[1].id)
    assert found is not None
    assert found.id == result.id
    assert web.thread_status(messages, "owner", {messages[1].id}) == "answered"


def test_status_waiting_for_a_task_without_my_result(client: Client, personal: Bus) -> None:
    task = personal.send("owner", "please decide", kind="task")
    row = thread_row(client, task.thread)
    assert row["status"] == "waiting"
    assert row["unread"] == 1


def test_a_text_reply_does_not_clear_waiting(client: Client, owner: Bus, personal: Bus) -> None:
    task = personal.send("owner", "please decide", kind="task")
    owner.send("personal", "one question first", kind="text", reply_to=task.id)
    assert thread_row(client, task.thread)["status"] == "waiting"


def test_opening_a_thread_acks_but_a_task_stays_waiting(client: Client, personal: Bus) -> None:
    task = personal.send("owner", "please decide", kind="task")
    open_thread(client, task.thread)
    row = thread_row(client, task.thread)
    assert row["unread"] == 0
    assert row["status"] == "waiting"


def test_status_new_for_an_unread_text(client: Client, personal: Bus) -> None:
    text = personal.send("owner", "fyi")
    row = thread_row(client, text.thread)
    assert row["status"] == "new"
    assert row["unread"] == 1


def test_status_answered_once_everything_is_read_and_no_task_is_open(
    client: Client, owner: Bus, personal: Bus
) -> None:
    text = personal.send("owner", "fyi")
    open_thread(client, text.thread)
    row = thread_row(client, text.thread)
    assert (row["status"], row["unread"]) == ("answered", 0)

    mine = owner.send("personal", "I started this")
    assert thread_row(client, mine.thread)["status"] == "answered"


def test_status_answered_after_a_result_reply(client: Client, personal: Bus) -> None:
    task = personal.send("owner", "ok?", kind="task")
    status, _ = client.post("/api/reply", {"msg_id": task.id, "kind": "result", "body": "да"})
    assert status == 200
    row = thread_row(client, task.thread)
    assert (row["status"], row["unread"]) == ("answered", 0)


def test_status_new_again_when_a_fresh_message_arrives_after_answering(
    client: Client, personal: Bus
) -> None:
    task = personal.send("owner", "ok?", kind="task")
    client.post("/api/reply", {"msg_id": task.id, "kind": "result", "body": "да"})
    personal.send("owner", "follow-up", kind="task", reply_to=task.id)
    # the follow-up is a task of its own without my result: waiting beats new
    assert thread_row(client, task.thread)["status"] == "waiting"


def test_unread_counts_only_messages_to_me_not_acked(
    client: Client, owner: Bus, personal: Bus
) -> None:
    first = personal.send("owner", "one")
    personal.send("owner", "two", reply_to=first.id)
    personal.send("owner", "three", reply_to=first.id)
    owner.send("personal", "mine does not count", reply_to=first.id)
    other = personal.send("owner", "other thread")
    owner.ack(first.id)
    assert thread_row(client, first.thread)["unread"] == 2
    assert thread_row(client, other.thread)["unread"] == 1


def test_listing_threads_does_not_ack(client: Client, owner: Bus, personal: Bus) -> None:
    msg = personal.send("owner", "still unread")
    threads_of(client)
    threads_of(client)
    assert [m.id for m in owner.inbox()] == [msg.id]


def test_thread_view_is_oldest_first_with_mine_flags(
    client: Client, owner: Bus, personal: Bus
) -> None:
    first = personal.send("owner", "question?")
    second = owner.send("personal", "my answer", reply_to=first.id)
    third = personal.send("owner", "thanks", reply_to=second.id)

    view = open_thread(client, first.thread)
    assert view["thread"] == first.thread
    assert view["counterpart"] == "personal"
    msgs = view["messages"]
    assert [m["id"] for m in msgs] == [first.id, second.id, third.id]
    assert [m["mine"] for m in msgs] == [False, True, False]
    assert [m["from"] for m in msgs] == ["personal", "owner", "personal"]
    assert [m["to"] for m in msgs] == ["owner", "personal", "owner"]
    assert [m["body"] for m in msgs] == ["question?", "my answer", "thanks"]
    assert [m["reply_to"] for m in msgs] == [None, first.id, second.id]
    assert [m["kind"] for m in msgs] == ["text", "text", "text"]
    assert all(re.match(r"\d{4}-\d{2}-\d{2}T", m["time"]) for m in msgs)
    assert all(m["attachments"] == [] for m in msgs)


def test_thread_view_lists_attachment_names(client: Client, personal: Bus, tmp_path: Path) -> None:
    f = tmp_path / "notes.txt"
    f.write_text("hello")
    msg = personal.send("owner", "see file", attachments=[f])
    view = open_thread(client, msg.thread)
    assert view["messages"][0]["attachments"] == ["notes.txt"]


def test_opening_a_thread_acks_only_that_threads_unread_to_me(
    client: Client, owner: Bus, personal: Bus
) -> None:
    a1 = personal.send("owner", "a1")
    a2 = personal.send("owner", "a2", reply_to=a1.id)
    b1 = personal.send("owner", "b1")
    assert {m.id for m in owner.inbox()} == {a1.id, a2.id, b1.id}

    open_thread(client, a1.thread)
    assert [m.id for m in owner.inbox()] == [b1.id]
    assert thread_row(client, a1.thread)["unread"] == 0
    assert thread_row(client, b1.thread)["unread"] == 1


def test_open_task_is_the_newest_unanswered_task_to_me(
    client: Client, owner: Bus, personal: Bus
) -> None:
    t1 = personal.send("owner", "first task", kind="task")
    t2 = personal.send("owner", "second task", kind="task", reply_to=t1.id)
    personal.send("owner", "a plain note", kind="text", reply_to=t1.id)
    assert open_thread(client, t1.thread)["open_task"] == {"id": t2.id}

    owner.send("personal", "да", kind="result", reply_to=t2.id)
    view = open_thread(client, t1.thread)
    assert view["open_task"] == {"id": t1.id}
    assert view["answered_task"] is None


def test_open_task_is_null_without_a_task(client: Client, personal: Bus) -> None:
    text = personal.send("owner", "just talk")
    view = open_thread(client, text.thread)
    assert view["open_task"] is None
    assert view["answered_task"] is None


def test_a_text_reply_leaves_the_task_open(client: Client, owner: Bus, personal: Bus) -> None:
    task = personal.send("owner", "decide", kind="task")
    owner.send("personal", "a remark", kind="text", reply_to=task.id)
    assert open_thread(client, task.thread)["open_task"] == {"id": task.id}


def test_answered_task_carries_my_result_body(client: Client, owner: Bus, personal: Bus) -> None:
    task = personal.send("owner", "decide", kind="task")
    owner.send("personal", "нет", kind="result", reply_to=task.id)
    view = open_thread(client, task.thread)
    assert view["open_task"] is None
    assert view["answered_task"] == {"id": task.id, "answer": "нет"}


def test_answered_task_is_the_newest_task_that_has_my_result(
    client: Client, owner: Bus, personal: Bus
) -> None:
    t1 = personal.send("owner", "first", kind="task")
    owner.send("personal", "да", kind="result", reply_to=t1.id)
    t2 = personal.send("owner", "second", kind="task", reply_to=t1.id)
    owner.send("personal", "нет", kind="result", reply_to=t2.id)
    view = open_thread(client, t1.thread)
    assert view["open_task"] is None
    assert view["answered_task"] == {"id": t2.id, "answer": "нет"}


def test_unknown_thread_is_a_400(client: Client) -> None:
    status, data = client.post("/api/thread", {"thread": "nope"})
    assert status == 400
    assert data["error"]


def test_a_thread_not_involving_me_is_a_400_and_acks_nothing(
    client: Client, owner_store: Store, owner: Bus, personal: Bus
) -> None:
    legion = Bus(owner_store, "legion", rate_limit=1000)
    foreign = legion.send("personal", "private to them")
    status, data = client.post("/api/thread", {"thread": foreign.thread})
    assert status == 400
    assert data["error"]
    assert "private to them" not in json.dumps(data)
    assert [m.id for m in personal.inbox()] == [foreign.id]


def test_thread_without_a_thread_field_is_a_400(client: Client) -> None:
    status, data = client.post("/api/thread", {})
    assert status == 400
    assert data["error"]


@pytest.mark.parametrize("kind", ["task", "text"])
def test_say_mode_reply_on_a_task_or_text(client: Client, personal: Bus, kind: str) -> None:
    msg = personal.send("owner", "hello", kind=kind)  # type: ignore[arg-type]
    view = open_thread(client, msg.thread)
    assert view["say"] == {"mode": "reply", "reply_to": msg.id}


def test_say_mode_reply_targets_the_latest_message_to_me(
    client: Client, owner: Bus, personal: Bus
) -> None:
    first = personal.send("owner", "one")
    personal.send("owner", "two", reply_to=first.id)
    last = personal.send("owner", "three", kind="task", reply_to=first.id)
    owner.send("personal", "my own newest message", reply_to=last.id)
    assert open_thread(client, first.thread)["say"] == {"mode": "reply", "reply_to": last.id}


@pytest.mark.parametrize("kind", ["result", "await_human"])
def test_say_mode_new_after_a_result_or_await_human(
    client: Client, personal: Bus, kind: str
) -> None:
    msg = personal.send("owner", "the end", kind=kind)  # type: ignore[arg-type]
    say = open_thread(client, msg.thread)["say"]
    assert say == {
        "mode": "new",
        "hint": "Ответ уйдёт новым сообщением: на результат ответить нельзя.",
    }


def test_say_mode_new_when_the_thread_has_no_incoming(client: Client, owner: Bus) -> None:
    mine = owner.send("personal", "I wrote first")
    say = open_thread(client, mine.thread)["say"]
    assert say["mode"] == "new"
    assert say["hint"].startswith("Ответ уйдёт новым сообщением")
    assert say["hint"].endswith("в этой ветке нет входящих.")


def test_say_in_mode_reply_sends_text_with_reply_to(client: Client, personal: Bus) -> None:
    msg = personal.send("owner", "tell me", kind="task")
    status, data = client.post("/api/say", {"thread": msg.thread, "body": "here you go"})
    assert status == 200
    assert data["mode"] == "reply"
    (got,) = personal.inbox()
    assert got.id == data["id"]
    assert (got.kind, got.body, got.reply_to) == ("text", "here you go", msg.id)
    assert got.sender == "owner"
    assert got.thread == msg.thread


def test_say_in_mode_reply_goes_to_the_original_sender(
    client: Client, owner_store: Store, personal: Bus
) -> None:
    msg = personal.send("owner", "hello")
    status, _ = client.post("/api/say", {"thread": msg.thread, "body": "hi"})
    assert status == 200
    assert [m.body for m in personal.inbox()] == ["hi"]


def test_say_in_mode_new_sends_a_fresh_message_to_the_counterpart(
    client: Client, owner: Bus, personal: Bus
) -> None:
    result = personal.send("owner", "all done", kind="result")
    status, data = client.post("/api/say", {"thread": result.thread, "body": "thanks, one more"})
    assert status == 200
    assert data["mode"] == "new"
    (got,) = personal.inbox()
    assert got.id == data["id"]
    assert (got.kind, got.body, got.sender, got.to) == (
        "text",
        "thanks, one more",
        "owner",
        "personal",
    )
    assert got.reply_to is None
    assert got.thread == got.id
    assert got.thread != result.thread


def test_say_in_mode_new_without_incoming_goes_to_the_counterpart(
    client: Client, owner_store: Store, owner: Bus
) -> None:
    legion = Bus(owner_store, "legion", rate_limit=1000)
    mine = owner.send("legion", "ping")
    status, data = client.post("/api/say", {"thread": mine.thread, "body": "ping again"})
    assert status == 200
    assert data["mode"] == "new"
    got = next(m for m in legion.inbox() if m.id == data["id"])
    assert (got.body, got.reply_to, got.kind, got.to) == ("ping again", None, "text", "legion")


def test_say_on_an_unknown_thread_or_empty_body_is_a_400(client: Client, personal: Bus) -> None:
    status, data = client.post("/api/say", {"thread": "nope", "body": "x"})
    assert status == 400
    assert data["error"]
    msg = personal.send("owner", "hi")
    status, data = client.post("/api/say", {"thread": msg.thread, "body": "   "})
    assert status == 400
    assert data["error"]
    assert personal.inbox() == []


def test_say_with_a_secret_is_refused_with_the_leak_scan_message(
    client: Client, personal: Bus
) -> None:
    secret = "gh" + "p_" + "A" * 36
    msg = personal.send("owner", "hi")
    status, data = client.post("/api/say", {"thread": msg.thread, "body": f"here: {secret}"})
    assert status == 400
    assert "GitHub token" in data["error"]
    assert secret not in data["error"]
    assert personal.inbox() == []


def test_a_second_result_to_the_same_task_is_a_409_and_stores_nothing(
    client: Client, personal: Bus
) -> None:
    task = personal.send("owner", "ok?", kind="task")
    status, _ = client.post("/api/reply", {"msg_id": task.id, "kind": "result", "body": "да"})
    assert status == 200
    status, data = client.post("/api/reply", {"msg_id": task.id, "kind": "result", "body": "нет"})
    assert status == 409
    assert data == {"error": "already answered"}
    results = [m for m in personal.inbox() if m.kind == "result"]
    assert [m.body for m in results] == ["да"]
    assert len(personal.inbox()) == 1


class _FakeHubApi:
    """A hub that assigns a seq and, like the real one, never touches any mirror on append."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def post_event(self, record: dict[str, Any]) -> dict[str, Any]:
        stored = {**record, "seq": len(self.records) + 1}
        self.records.append(stored)
        return stored

    def events(self, after: int, limit: int = 100) -> list[bytes]:
        return [json.dumps(r).encode() for r in self.records if r["seq"] > after][:limit]


class _NoRegistry:
    def teammates(self, agent: str) -> None:
        return None

    def report_activity(self, agent: str, report: object) -> None:
        return None


def _hub_bus(api: _FakeHubApi, home: Path, name: str) -> tuple[Bus, HubStore]:
    store = HubStore(api, home)  # type: ignore[arg-type]
    return Bus(store, name, registry=_NoRegistry(), rate_limit=1000), store  # type: ignore[arg-type]


def test_on_a_hub_a_second_answer_is_a_409_before_any_poll_syncs(tmp_path: Path) -> None:
    api = _FakeHubApi()
    me, me_store = _hub_bus(api, tmp_path / "me", "owner")
    other, _ = _hub_bus(api, tmp_path / "other", "personal")
    task = other.send("owner", "ok?", kind="task")
    server = make_server(me, port=0, sync=me_store.sync)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        c = Client(server)
        assert [t["status"] for t in c.get_json("/api/threads")["threads"]] == ["waiting"]
        first = {"msg_id": task.id, "kind": "result", "body": "да"}
        assert c.post("/api/reply", first)[0] == 200
        # No poll in between: the mirror does not hold the first answer until a sync.
        second = {"msg_id": task.id, "kind": "result", "body": "нет"}
        status, data = c.post("/api/reply", second)
        assert (status, data) == (409, {"error": "already answered"})
        results = [r["body"] for r in api.records if r.get("kind") == "result"]
        assert results == ["да"]
        # The view right after answering shows the answer, not the radios again.
        view = c.post("/api/thread", {"thread": task.thread})[1]
        assert view["open_task"] is None
        assert view["answered_task"] == {"id": task.id, "answer": "да"}
        # Reopening acks once: the repeated opens add no second AckEvent.
        c.post("/api/thread", {"thread": task.thread})
        acks = [r for r in api.records if r["type"] == "ack" and r["id"] == task.id]
        assert len(acks) == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_a_text_reply_after_a_result_is_still_allowed(client: Client, personal: Bus) -> None:
    task = personal.send("owner", "ok?", kind="task")
    status, _ = client.post("/api/reply", {"msg_id": task.id, "kind": "result", "body": "да"})
    assert status == 200
    status, _ = client.post("/api/reply", {"msg_id": task.id, "kind": "text", "body": "and also"})
    assert status == 200
    assert [(m.kind, m.body) for m in personal.inbox()] == [("result", "да"), ("text", "and also")]


def test_a_result_to_another_task_is_not_blocked_by_an_earlier_answer(
    client: Client, personal: Bus
) -> None:
    t1 = personal.send("owner", "first", kind="task")
    t2 = personal.send("owner", "second", kind="task", reply_to=t1.id)
    assert client.post("/api/reply", {"msg_id": t1.id, "kind": "result", "body": "да"})[0] == 200
    assert client.post("/api/reply", {"msg_id": t2.id, "kind": "result", "body": "нет"})[0] == 200


def test_send_still_opens_a_new_thread_that_appears_in_the_list(
    client: Client, personal: Bus
) -> None:
    status, data = client.post("/api/send", {"body": "hello personal\nmore"})
    assert status == 200
    row = thread_row(client, data["id"])
    assert row["title"] == "hello personal"
    assert row["counterpart"] == "personal"
    assert row["status"] == "answered"


def test_threads_sync_runs_first_and_failure_is_a_503(serve: ServerFactory, personal: Bus) -> None:
    class HubDownError(Exception):
        pass

    def deliver() -> None:
        personal.send("owner", "arrived in sync")

    client = serve(sync=deliver)
    assert [t["title"] for t in threads_of(client)] == ["arrived in sync"]

    def boom() -> None:
        raise HubDownError("secret detail")

    failing = serve(sync=boom)
    status, _, data = failing.request("GET", "/api/threads")
    assert status == 503
    assert json.loads(data) == {"error": "HubDownError"}


def test_wrong_host_header_is_forbidden_on_threads_and_thread_posts(client: Client) -> None:
    for host in ("evil.example", f"evil.example:{client.port}", "127.0.0.1:1", "127.0.0.1"):
        status, _, _ = client.request("GET", "/api/threads", host=host)
        assert status == 403, host
        for path in ("/api/thread", "/api/say"):
            status, _, _ = client.request("POST", path, {"thread": "x", "body": "x"}, host=host)
            assert status == 403, (host, path)


def test_localhost_host_header_is_accepted_on_threads(client: Client) -> None:
    status, _, _ = client.request("GET", "/api/threads", host=f"localhost:{client.port}")
    assert status == 200


def test_threads_carry_the_security_headers(client: Client) -> None:
    status, headers, _ = client.request("GET", "/api/threads")
    assert status == 200
    assert headers["Content-Security-Policy"] == "default-src 'self'"
    assert headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize("csrf", [None, "wrong-token"])
@pytest.mark.parametrize(
    ("path", "payload_of"),
    [
        ("/api/thread", lambda m: {"thread": m.thread}),
        ("/api/say", lambda m: {"thread": m.thread, "body": "sneaky"}),
    ],
)
def test_thread_and_say_without_the_csrf_token_are_forbidden(
    client: Client,
    owner: Bus,
    personal: Bus,
    path: str,
    payload_of: Callable[[Any], dict[str, str]],
    csrf: str | None,
) -> None:
    msg = personal.send("owner", "hello")
    status, _, _ = client.request("POST", path, payload_of(msg), csrf=csrf)
    assert status == 403
    assert [m.id for m in owner.inbox()] == [msg.id]
    assert [m.id for m in personal.inbox()] == []


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
