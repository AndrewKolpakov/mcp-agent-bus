"""`agent-bus web`: a local page where a human reads and answers their bus messages.

It is the same bus operations the CLI uses (`Bus.send`, `Bus.ack`), so the leak
scan, the reply rules and the rate limit apply unchanged. The server binds to
loopback only; the Host header and a per-process CSRF token guard against a
web page in the owner's browser reaching it (DNS rebinding, cross-site POST).
"""

from __future__ import annotations

import http.server
import json
import secrets
import sys
import threading
from collections.abc import Callable
from typing import Any, cast

from agent_bus import config
from agent_bus.backend import open_backend
from agent_bus.bus import Bus, BusError
from agent_bus.models import Kind, MessageEvent
from agent_bus.state import BusState

LOOPBACK_HOSTS = ("127.0.0.1", "localhost")
DEFAULT_PORT = 8765
DEFAULT_PEER = "personal"
READ_SHOWN = 20
"""How many already-read messages the inbox returns after every unread one."""
MAX_BODY_BYTES = 1024 * 1024

_NO_REPLY_KINDS = frozenset({"result", "await_human"})


def reply_kinds(message: MessageEvent) -> list[str]:
    """The kinds an answer to `message` may have; none for a message that only gets acked."""
    if message.kind in _NO_REPLY_KINDS:
        return []
    return ["text", "result"] if message.kind == "task" else ["text"]


TITLE_MAX = 120
NO_INCOMING_HINT = "Ответ уйдёт новым сообщением: в этой ветке нет входящих."
RESULT_HINT = "Ответ уйдёт новым сообщением: на результат ответить нельзя."


def thread_counterpart(messages: list[MessageEvent], me: str) -> str:
    """The other party of the opening message; `messages` is in log order."""
    opening = messages[0]
    return opening.to if opening.sender == me else opening.sender


def _my_results(messages: list[MessageEvent], me: str) -> set[str]:
    return {
        m.reply_to
        for m in messages
        if m.sender == me and m.kind == "result" and m.reply_to is not None
    }


def thread_status(messages: list[MessageEvent], me: str, acked: set[str]) -> str:
    """Waiting while a task to me lacks my result, else new while unacked, else answered."""
    answered = _my_results(messages, me)
    if any(m.to == me and m.kind == "task" and m.id not in answered for m in messages):
        return "waiting"
    if any(m.to == me and m.id not in acked for m in messages):
        return "new"
    return "answered"


def answered_result(messages: list[MessageEvent], me: str, task_id: str) -> MessageEvent | None:
    """My result reply to `task_id`, if any."""
    for m in messages:
        if m.sender == me and m.kind == "result" and m.reply_to == task_id:
            return m
    return None


def _say_target(messages: list[MessageEvent], me: str) -> dict[str, str]:
    incoming = [m for m in messages if m.to == me]
    if not incoming:
        return {"mode": "new", "hint": NO_INCOMING_HINT}
    latest = incoming[-1]
    if latest.kind in ("task", "text"):
        return {"mode": "reply", "reply_to": latest.id}
    return {"mode": "new", "hint": RESULT_HINT}


class WebServer(http.server.ThreadingHTTPServer):
    bus: Bus
    sync: Callable[[], object] | None
    peer: str
    hub: str | None
    csrf: str
    lock: threading.Lock
    daemon_threads = True


class _Handler(http.server.BaseHTTPRequestHandler):
    server: WebServer

    def log_message(self, format: str, *args: Any) -> None:
        """Silent: no request line and no body reaches any log."""

    # -- plumbing ---------------------------------------------------------

    def _send(self, status: int, payload: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Content-Security-Policy", "default-src 'self'")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(payload)

    def _json(self, status: int, data: object) -> None:
        self._send(status, json.dumps(data).encode(), "application/json; charset=utf-8")

    def _host_ok(self) -> bool:
        port = self.server.server_address[1]
        host = self.headers.get("Host", "")
        return host in {f"{name}:{port}" for name in LOOPBACK_HOSTS}

    def _read_json(self) -> dict[str, Any] | None:
        """The JSON object body, or None after answering 400/413 itself."""
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._json(400, {"error": "Content-Length is required"})
            return None
        if length < 0:
            self._json(400, {"error": "bad Content-Length"})
            return None
        if length > MAX_BODY_BYTES:
            self.close_connection = True
            self._json(413, {"error": "request body too large"})
            return None
        try:
            data = json.loads(self.rfile.read(length))
        except (ValueError, UnicodeDecodeError):
            self._json(400, {"error": "body must be JSON"})
            return None
        if not isinstance(data, dict):
            self._json(400, {"error": "body must be a JSON object"})
            return None
        return data

    # -- routes -----------------------------------------------------------

    def do_GET(self) -> None:
        if not self._host_ok():
            self._json(403, {"error": "forbidden host"})
            return
        path = self.path.split("?", 1)[0]
        if path == "/":
            page = PAGE.replace("__CSRF__", self.server.csrf).encode()
            self._send(200, page, "text/html; charset=utf-8")
        elif path == "/app.js":
            self._send(200, APP_JS.encode(), "text/javascript; charset=utf-8")
        elif path == "/app.css":
            self._send(200, APP_CSS.encode(), "text/css; charset=utf-8")
        elif path == "/api/inbox":
            self._guarded(self._inbox)
        elif path == "/api/threads":
            self._guarded(self._threads)
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if not self._host_ok():
            self._json(403, {"error": "forbidden host"})
            return
        supplied = self.headers.get("X-CSRF-Token", "")
        if not secrets.compare_digest(supplied.encode(), self.server.csrf.encode()):
            self._json(403, {"error": "bad CSRF token"})
            return
        routes = {
            "/api/reply": self._reply,
            "/api/send": self._new,
            "/api/ack": self._ack,
            "/api/thread": self._thread,
            "/api/say": self._say,
        }
        handler = routes.get(self.path.split("?", 1)[0])
        if handler is None:
            self._json(404, {"error": "not found"})
            return
        data = self._read_json()
        if data is not None:
            self._guarded(lambda: handler(data))

    def _guarded(self, action: Callable[[], None]) -> None:
        try:
            with self.server.lock:
                action()
        except _SyncError as exc:
            self._json(503, {"error": exc.name})
        except BusError as exc:
            self._json(400, {"error": str(exc)})

    def _sync(self) -> None:
        sync = self.server.sync
        if sync is not None:
            try:
                sync()
            except Exception as exc:
                raise _SyncError(type(exc).__name__) from None

    def _backend(self) -> dict[str, str | None]:
        hub = self.server.hub
        return {"mode": "hub", "hub": hub} if hub else {"mode": "local", "hub": None}

    def _my_threads(self) -> tuple[BusState, dict[str, list[MessageEvent]]]:
        """Threads that involve me, each in log order."""
        bus = self.server.bus
        state = bus.state()
        threads: dict[str, list[MessageEvent]] = {}
        for m in sorted(state.messages.values(), key=lambda m: state.position[m.id]):
            if m.sender == bus.me or m.to == bus.me:
                threads.setdefault(m.thread, []).append(m)
        return state, threads

    def _threads(self) -> None:
        self._sync()
        server = self.server
        me = server.bus.me
        state, threads = self._my_threads()
        rows = []
        for thread, messages in threads.items():
            title = messages[0].body.strip().split("\n", 1)[0].strip()[:TITLE_MAX]
            rows.append(
                {
                    "thread": thread,
                    "counterpart": thread_counterpart(messages, me),
                    "title": title,
                    "status": thread_status(messages, me, state.acked),
                    "unread": sum(1 for m in messages if m.to == me and m.id not in state.acked),
                    "last_time": max(m.ts for m in messages).isoformat(),
                }
            )
        rows.sort(key=lambda r: str(r["last_time"]), reverse=True)
        self._json(
            200,
            {"me": me, "peer": server.peer, "backend": self._backend(), "threads": rows},
        )

    def _thread_messages(self, data: dict[str, Any]) -> list[MessageEvent]:
        thread = self._text(data, "thread")
        _, threads = self._my_threads()
        messages = threads.get(thread)
        if not messages:
            raise BusError(f"unknown thread: {thread}")
        return messages

    def _thread(self, data: dict[str, Any]) -> None:
        self._sync()
        bus = self.server.bus
        me = bus.me
        messages = self._thread_messages(data)
        acked = bus.state().acked
        for m in messages:
            if m.to == me and m.id not in acked:
                bus.ack(m.id)
        answered = _my_results(messages, me)
        tasks = [m for m in messages if m.to == me and m.kind == "task"]
        open_tasks = [m for m in tasks if m.id not in answered]
        open_task = {"id": open_tasks[-1].id} if open_tasks else None
        answered_task = None
        if open_task is None:
            for task in reversed(tasks):
                result = answered_result(messages, me, task.id)
                if result is not None:
                    answered_task = {"id": task.id, "answer": result.body}
                    break
        self._json(
            200,
            {
                "thread": messages[0].thread,
                "counterpart": thread_counterpart(messages, me),
                "messages": [
                    {
                        "id": m.id,
                        "kind": m.kind,
                        "from": m.sender,
                        "to": m.to,
                        "time": m.ts.isoformat(),
                        "body": m.body,
                        "reply_to": m.reply_to,
                        "attachments": [a.name for a in m.attachments or ()],
                        "mine": m.sender == me,
                    }
                    for m in messages
                ],
                "open_task": open_task,
                "answered_task": answered_task,
                "say": _say_target(messages, me),
            },
        )

    def _say(self, data: dict[str, Any]) -> None:
        self._sync()
        bus = self.server.bus
        body = self._text(data, "body")
        messages = self._thread_messages(data)
        target = _say_target(messages, bus.me)
        if target["mode"] == "reply":
            original = bus.state().messages[target["reply_to"]]
            sent = bus.send(to=original.sender, body=body, kind="text", reply_to=original.id)
        else:
            sent = bus.send(to=thread_counterpart(messages, bus.me), body=body, kind="text")
        self._json(200, {"id": sent.id, "mode": target["mode"]})

    def _inbox(self) -> None:
        server = self.server
        self._sync()
        me = server.bus.me
        state = server.bus.state()
        mine = state.inbox(me)
        unread = [m for m in reversed(mine) if m.id not in state.acked]
        read = [m for m in reversed(mine) if m.id in state.acked][:READ_SHOWN]
        answers: dict[str, list[MessageEvent]] = {}
        for message in sorted(state.messages.values(), key=lambda m: state.position[m.id]):
            if message.sender == me and message.reply_to is not None:
                answers.setdefault(message.reply_to, []).append(message)
        shown = [
            {
                "id": m.id,
                "kind": m.kind,
                "from": m.sender,
                "time": m.ts.isoformat(),
                "body": m.body,
                "reply_to": m.reply_to,
                "thread": m.thread,
                "attachments": [a.name for a in m.attachments or ()],
                "unread": m.id not in state.acked,
                "answerable": bool(reply_kinds(m)),
                "reply_kinds": reply_kinds(m),
                "replies": [
                    {"id": r.id, "kind": r.kind, "time": r.ts.isoformat(), "body": r.body}
                    for r in answers.get(m.id, [])
                ],
            }
            for m in [*unread, *read]
        ]
        self._json(
            200,
            {"me": me, "peer": server.peer, "backend": self._backend(), "messages": shown},
        )

    def _text(self, data: dict[str, Any], key: str) -> str:
        value = data.get(key)
        if not isinstance(value, str):
            raise BusError(f"{key} must be a string")
        return value

    def _reply(self, data: dict[str, Any]) -> None:
        msg_id = self._text(data, "msg_id")
        kind = self._text(data, "kind")
        body = self._text(data, "body")
        # On a hub an append never reaches the mirror, so the answered check needs a pull.
        self._sync()
        bus = self.server.bus
        original = bus.state().messages.get(msg_id)
        if original is None or original.to != bus.me:
            raise BusError(f"unknown message id: {msg_id}")
        if kind not in reply_kinds(original):
            raise BusError(f"kind {kind!r} is not allowed for an answer to message {msg_id}")
        everything = list(bus.state().messages.values())
        if kind == "result" and answered_result(everything, bus.me, msg_id):
            self._json(409, {"error": "already answered"})
            return
        sent = bus.send(to=original.sender, body=body, kind=cast(Kind, kind), reply_to=msg_id)
        self._json(200, {"id": sent.id})

    def _new(self, data: dict[str, Any]) -> None:
        body = self._text(data, "body")
        sent = self.server.bus.send(to=self.server.peer, body=body, kind="text")
        self._json(200, {"id": sent.id})

    def _ack(self, data: dict[str, Any]) -> None:
        ids = data.get("msg_ids")
        if not isinstance(ids, list) or not all(isinstance(i, str) for i in ids):
            raise BusError("msg_ids must be a list of strings")
        acked = [msg_id for msg_id in ids if self.server.bus.ack(msg_id)]
        self._json(200, {"acked": acked})


class _SyncError(Exception):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.name = name


def make_server(
    bus: Bus,
    *,
    host: str = "127.0.0.1",
    port: int = DEFAULT_PORT,
    sync: Callable[[], object] | None = None,
    peer: str = DEFAULT_PEER,
    hub: str | None = None,
) -> WebServer:
    if host not in LOOPBACK_HOSTS:
        raise ValueError(f"web binds to loopback only ({', '.join(LOOPBACK_HOSTS)}), not {host!r}")
    server = WebServer((host, port), _Handler)
    server.bus = bus
    server.sync = sync
    server.peer = peer
    server.hub = hub
    server.csrf = secrets.token_urlsafe(32)
    server.lock = threading.Lock()
    return server


def run(cfg: config.Config, *, port: int = DEFAULT_PORT) -> int:
    from agent_bus.remote import HubStore

    backend = open_backend(cfg)
    bus = Bus(
        backend.store,
        cfg.require_agent(),
        registry=backend.registry,
        rate_limit=cfg.rate_limit,
        rate_window=cfg.rate_window,
    )
    store = backend.store
    is_hub = isinstance(store, HubStore)
    sync = store.sync if isinstance(store, HubStore) else None
    hub = cfg.hub if is_hub else None
    server = make_server(bus, port=port, sync=sync, hub=hub)
    start = f"agent-bus web: http://127.0.0.1:{server.server_address[1]}/ as {bus.me}"
    if hub:
        print(f"{start} — hub {hub}", flush=True)
    else:
        print(f"{start} — LOCAL bus (not connected to a hub; set AGENT_BUS_HUB)", flush=True)
        print(
            "WARNING: LOCAL bus: messages stay on this machine and never reach a hub."
            " Set AGENT_BUS_HUB (or the plugin's hub_url) and restart.",
            file=sys.stderr,
            flush=True,
        )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="csrf" content="__CSRF__">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>agent-bus</title>
<link rel="stylesheet" href="/app.css">
</head>
<body>
<main id="app"></main>
<script src="/app.js"></script>
</body>
</html>
"""

APP_CSS = """\
body { font: 15px/1.4 system-ui, sans-serif; margin: 0; background: #f6f6f4; color: #1b1b1b; }
main { display: flex; flex-direction: column; height: 100vh; box-sizing: border-box;
  padding: .6rem .8rem; }
h1 { font-size: 1.2rem; margin: 0 0 .5rem; }
.card { background: #fff; border: 1px solid #d8d8d4; border-radius: 6px; padding: .6rem .8rem;
  margin-bottom: .7rem; }
.meta { color: #555; font-size: .85rem; margin-bottom: .3rem; }
.body { white-space: pre-wrap; word-break: break-word; }
.row { display: flex; gap: .5rem; margin-top: .5rem; align-items: flex-start; }
.panes { display: flex; gap: .8rem; align-items: stretch; flex: 1 1 auto; min-height: 0; }
.list { flex: 0 0 340px; min-width: 0; overflow-y: auto; }
.detail { flex: 1 1 0; min-width: 0; min-height: 0; display: flex; flex-direction: column;
  background: #fff; border: 1px solid #d8d8d4; border-radius: 6px; }
.detail-head { flex: 0 0 auto; padding: .5rem .8rem; min-width: 0;
  border-bottom: 1px solid #d8d8d4; }
.detail-title { font-size: 1.05rem; overflow: hidden; text-overflow: ellipsis;
  white-space: nowrap; min-width: 0; }
.detail-empty { flex: 1 1 auto; display: flex; align-items: center; justify-content: center;
  color: #777; }
.reply { flex: 0 0 auto; border-top: 1px solid #d8d8d4; padding: .5rem .8rem; background: #fafaf8; }
.reply .row { margin-top: .4rem; }
.thread { border-bottom: 1px solid #e3e3df; padding: .4rem .6rem; cursor: pointer; min-width: 0; }
.thread:hover { background: #ececE8; }
.list .card { position: sticky; bottom: 0; margin: 0; border-radius: 0; }
.thread.selected { background: #dbe8ff; }
.thread .title { font-weight: 400; font-size: 15px; overflow: hidden; text-overflow: ellipsis;
  white-space: nowrap; min-width: 0; }
.thread .meta { margin: 0; }
.badge { font-size: .75rem; border-radius: 9px; padding: 0 .5rem; background: #e5e5e0; }
.badge.waiting { background: #fde7c3; }
.badge.new { background: #cfe0ff; }
.unread-count { font-size: .75rem; font-weight: 700; color: #fff; background: #2563eb;
  border-radius: 9px; padding: 0 .4rem; margin-left: .3rem; }
.msgs { flex: 1 1 auto; min-height: 0; overflow-y: auto; display: flex; flex-direction: column;
  gap: .6rem; padding: .6rem .8rem; }
.msg { max-width: 70%; align-self: flex-start; min-width: 0; }
.msg.mine { align-self: flex-end; }
.msg .bubble { background: #f0f0ec; border: 1px solid #d8d8d4; border-radius: 10px;
  padding: .4rem .6rem; }
.msg.mine .bubble { background: #dbe8ff; border-color: #b6c8e8; }
.msg .meta { font-size: .75rem; margin: .1rem .3rem 0; }
.msg .kind { color: #777; font-size: .75rem; }
@media (max-width: 800px) {
  body { overflow: auto; }
  main { height: auto; }
  .panes { flex-direction: column; flex: none; }
  .list { flex: 0 0 auto; max-height: 40vh; }
  .detail { flex: none; min-height: 60vh; }
  .msgs { max-height: 50vh; }
  .msg { max-width: 90%; }
}
textarea { width: 100%; min-height: 3.5rem; font: inherit; box-sizing: border-box; }
.banner-local { background: #b00020; color: #fff; padding: .6rem .8rem; border-radius: 6px;
  margin: 0 0 .7rem; font-weight: 600; }
.hub { color: #555; font-size: .85rem; margin: 0 0 .5rem; }
.error { color: #b00020; margin: .4rem 0; white-space: pre-wrap; }
button { font: inherit; padding: .3rem .8rem; cursor: pointer; }
"""

APP_JS = """\
(() => {
  const csrf = document.querySelector('meta[name="csrf"]').content;
  const root = document.getElementById('app');
  const state = {
    data: null, view: null, selected: null, error: '',
    choice: '', choiceFor: null, say: '', newBody: '',
  };
  const statusText = { waiting: 'ждёт ответа', new: 'новое', answered: 'отвечено' };

  const el = (tag, attrs, ...children) => {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs || {})) {
      if (key === 'class') node.className = value;
      else node[key] = value;
    }
    for (const child of children) {
      node.append(typeof child === 'string' ? document.createTextNode(child) : child);
    }
    return node;
  };

  const call = async (path, payload) => {
    const options = payload === undefined
      ? { cache: 'no-store' }
      : {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf },
          body: JSON.stringify(payload),
        };
    const response = await fetch(path, options);
    let data = {};
    try { data = await response.json(); } catch (e) { data = {}; }
    if (!response.ok) throw new Error(data.error || ('HTTP ' + response.status));
    return data;
  };

  const act = async (path, payload, done) => {
    try {
      const result = await call(path, payload);
      state.error = '';
      if (done) done(result);
      if (state.selected) await openThread(state.selected);
      else await load();
    } catch (e) {
      state.error = String(e.message || e);
      render();
    }
  };

  const openThread = async (thread) => {
    try {
      const view = await call('/api/thread', { thread: thread });
      if (state.selected !== thread) { state.choice = ''; state.say = ''; }
      state.selected = thread;
      state.view = view;
      state.error = '';
    } catch (e) {
      state.error = String(e.message || e);
    }
    await load();
  };

  const load = async () => {
    try {
      state.data = await call('/api/threads');
      state.error = '';
    } catch (e) {
      state.error = String(e.message || e);
    }
    render();
  };

  // One tick: list, then (only when a thread is open) that thread's view, which acks
  // its new messages because they are on screen; the list is read again so the open
  // thread's badge shows the ack. A failed thread call keeps the old view.
  const poll = async () => {
    const thread = state.selected;
    await load();
    if (!thread || state.selected !== thread) return;
    try {
      const view = await call('/api/thread', { thread: thread });
      if (state.selected !== thread) return;
      state.view = view;
      if (!view.open_task) state.choice = '';
    } catch (e) {
      state.error = String(e.message || e);
      render();
      return;
    }
    await load();
  };

  const threadRow = (t) => {
    const node = el('div', { class: 'thread' + (t.thread === state.selected ? ' selected' : '') });
    node.append(el('div', { class: 'title' }, t.title || '(без текста)'));
    const meta = el('div', { class: 'meta' }, t.counterpart + ' ');
    meta.append(el('span', { class: 'badge ' + t.status }, statusText[t.status] || t.status));
    if (t.unread > 0) meta.append(el('span', { class: 'unread-count' }, String(t.unread)));
    node.append(meta);
    node.addEventListener('click', () => openThread(t.thread));
    return node;
  };

  const message = (m) => {
    const node = el('div', { class: 'msg' + (m.mine ? ' mine' : '') });
    const bubble = el('div', { class: 'bubble' }, el('div', { class: 'body' }, m.body));
    if (m.attachments.length) {
      bubble.append(el('div', { class: 'meta' }, 'attachments: ' + m.attachments.join(', ')));
    }
    node.append(bubble);
    node.append(el('div', { class: 'meta' }, m.from + ' · ' + m.time + ' · ',
      el('span', { class: 'kind' }, m.kind)));
    return node;
  };

  const answerBox = (view) => {
    if (view.open_task) {
      const taskId = view.open_task.id;
      // A choice belongs to the task it was made for: when a newer task becomes the open
      // one, the radios start empty, so one click never answers a task not yet read.
      if (state.choiceFor !== taskId) { state.choice = ''; state.choiceFor = taskId; }
      const answer = el('button', {}, 'Ответить');
      answer.disabled = !state.choice;
      const choices = el('div', { class: 'row' });
      for (const [label, value] of [['Да', 'да'], ['Нет', 'нет']]) {
        const radio = el('input', { type: 'radio', name: 'answer-' + taskId, value: value });
        radio.checked = state.choice === value;
        radio.addEventListener('change', () => {
          state.choice = value;
          state.choiceFor = taskId;
          answer.disabled = false;
        });
        choices.append(el('label', {}, radio, label));
      }
      answer.addEventListener('click', () => act(
        '/api/reply',
        { msg_id: taskId, kind: 'result', body: state.choice },
        () => { state.choice = ''; },
      ));
      choices.append(answer);
      return choices;
    }
    if (view.answered_task) {
      return el('div', { class: 'meta' }, 'Вы ответили: ' + view.answered_task.answer);
    }
    return null;
  };

  const detail = (view) => {
    const pane = el('div', { class: 'detail' });
    if (!view) {
      pane.append(el('div', { class: 'detail-empty' }, 'Выберите ветку слева.'));
      return pane;
    }
    const row = ((state.data && state.data.threads) || [])
      .find((t) => t.thread === state.selected);
    const head = el('div', { class: 'detail-head' },
      el('div', { class: 'detail-title' }, (row && row.title) || '(без текста)'),
      el('div', { class: 'meta' }, 'Ветка: ' + view.counterpart));
    pane.append(head);
    const list = el('div', { class: 'msgs', id: 'msgs' });
    for (const m of view.messages) list.append(message(m));
    pane.append(list);
    const reply = el('div', { class: 'reply' });
    const box = answerBox(view);
    if (box) reply.append(box);
    if (view.say.mode === 'new') reply.append(el('div', { class: 'meta' }, view.say.hint));
    const area = el('textarea', {
      placeholder: 'Сообщение для ' + view.counterpart, id: 'say-area', rows: 3,
    });
    area.value = state.say;
    area.addEventListener('input', () => { state.say = area.value; });
    const send = el('button', {}, 'Отправить');
    send.addEventListener('click', () => act(
      '/api/say', { thread: view.thread, body: area.value }, () => { state.say = ''; },
    ));
    reply.append(el('div', { class: 'row' }, area), el('div', { class: 'row' }, send));
    pane.append(reply);
    return pane;
  };

  const render = () => {
    // The drafts and the radio choice live in state, so a rebuild loses only the DOM
    // position: capture the scroll offsets and the caret first, put them back after.
    const msgs = document.getElementById('msgs');
    const pageTop = window.scrollY;
    const paneTop = msgs ? msgs.scrollTop : 0;
    const nearBottom = !msgs || (msgs.scrollHeight - msgs.scrollTop - msgs.clientHeight < 24);
    const sameThread = state.shownThread === state.selected;
    const active = document.activeElement;
    const focus = active && active.tagName === 'TEXTAREA' && active.id
      ? { id: active.id, start: active.selectionStart, end: active.selectionEnd }
      : null;
    draw();
    state.shownThread = state.selected;
    const fresh = document.getElementById('msgs');
    if (fresh) {
      fresh.scrollTop = (nearBottom || !sameThread) ? fresh.scrollHeight : paneTop;
    }
    window.scrollTo(0, pageTop);
    if (focus) {
      const area = document.getElementById(focus.id);
      if (area) {
        area.focus({ preventScroll: true });
        area.setSelectionRange(focus.start, focus.end);
      }
    }
  };

  const draw = () => {
    const data = state.data;
    root.replaceChildren();
    root.append(el('h1', {}, data ? 'agent-bus: ' + data.me : 'agent-bus'));
    if (data && data.backend) {
      if (data.backend.mode === 'hub') {
        root.append(el('div', { class: 'hub' }, 'hub: ' + data.backend.hub));
      } else {
        root.append(el('div', { class: 'banner-local' },
          "LOCAL bus — not connected to a hub. Messages stay on this machine. "
            + "Set AGENT_BUS_HUB (or the plugin's hub_url) and restart."));
      }
    }
    const err = el('div', { class: 'error', id: 'error' }, state.error);
    root.append(err);
    if (!data) return;

    const area = el('textarea', {
      placeholder: 'Новое сообщение для ' + data.peer, id: 'new-area',
    });
    area.value = state.newBody;
    area.addEventListener('input', () => { state.newBody = area.value; });
    const send = el('button', {}, 'Отправить');
    send.addEventListener('click', () => act(
      '/api/send', { body: area.value }, () => { state.newBody = ''; },
    ));
    const compose = el('div', { class: 'card' },
      el('div', { class: 'meta' }, 'Написать ' + data.peer),
      area, el('div', { class: 'row' }, send));

    const list = el('div', { class: 'list' });
    if (!data.threads.length) list.append(el('div', { class: 'meta' }, 'Нет веток.'));
    for (const t of data.threads) list.append(threadRow(t));
    list.append(compose);
    root.append(el('div', { class: 'panes' }, list, detail(state.view)));
  };

  load();
  setInterval(poll, 10000);
})();
"""
