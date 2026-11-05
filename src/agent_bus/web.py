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
import threading
from collections.abc import Callable
from typing import Any, cast

from agent_bus import config
from agent_bus.backend import open_backend
from agent_bus.bus import Bus, BusError
from agent_bus.models import Kind, MessageEvent

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


class WebServer(http.server.ThreadingHTTPServer):
    bus: Bus
    sync: Callable[[], object] | None
    peer: str
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
        routes = {"/api/reply": self._reply, "/api/send": self._new, "/api/ack": self._ack}
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

    def _inbox(self) -> None:
        server = self.server
        if server.sync is not None:
            try:
                server.sync()
            except Exception as exc:
                raise _SyncError(type(exc).__name__) from None
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
        self._json(200, {"me": me, "peer": server.peer, "messages": shown})

    def _text(self, data: dict[str, Any], key: str) -> str:
        value = data.get(key)
        if not isinstance(value, str):
            raise BusError(f"{key} must be a string")
        return value

    def _reply(self, data: dict[str, Any]) -> None:
        msg_id = self._text(data, "msg_id")
        kind = self._text(data, "kind")
        body = self._text(data, "body")
        bus = self.server.bus
        original = bus.state().messages.get(msg_id)
        if original is None or original.to != bus.me:
            raise BusError(f"unknown message id: {msg_id}")
        if kind not in reply_kinds(original):
            raise BusError(f"kind {kind!r} is not allowed for an answer to message {msg_id}")
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
) -> WebServer:
    if host not in LOOPBACK_HOSTS:
        raise ValueError(f"web binds to loopback only ({', '.join(LOOPBACK_HOSTS)}), not {host!r}")
    server = WebServer((host, port), _Handler)
    server.bus = bus
    server.sync = sync
    server.peer = peer
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
    sync = store.sync if isinstance(store, HubStore) else None
    server = make_server(bus, port=port, sync=sync)
    print(
        f"agent-bus web: http://127.0.0.1:{server.server_address[1]}/ as {bus.me}",
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
main { max-width: 52rem; margin: 0 auto; padding: 1rem; }
h1 { font-size: 1.2rem; margin: 0 0 .5rem; }
.bar { display: flex; gap: .5rem; align-items: center; margin: .5rem 0 1rem; }
.card { background: #fff; border: 1px solid #d8d8d4; border-radius: 6px; padding: .6rem .8rem;
  margin-bottom: .7rem; }
.card.unread { border-left: 4px solid #2563eb; }
.meta { color: #555; font-size: .85rem; margin-bottom: .3rem; }
.body { white-space: pre-wrap; word-break: break-word; }
.reply { margin: .5rem 0 0 1rem; padding-left: .6rem; border-left: 3px solid #b6c8e8; }
.row { display: flex; gap: .5rem; margin-top: .5rem; align-items: flex-start; }
textarea { width: 100%; min-height: 3.5rem; font: inherit; box-sizing: border-box; }
.error { color: #b00020; margin: .4rem 0; white-space: pre-wrap; }
button { font: inherit; padding: .3rem .8rem; cursor: pointer; }
"""

APP_JS = """\
(() => {
  const csrf = document.querySelector('meta[name="csrf"]').content;
  const root = document.getElementById('app');
  const state = { data: null, error: '', drafts: {}, kinds: {}, newBody: '' };

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
      await load();
    } catch (e) {
      state.error = String(e.message || e);
      render();
    }
  };

  const load = async () => {
    try {
      state.data = await call('/api/inbox');
      state.error = '';
    } catch (e) {
      state.error = String(e.message || e);
    }
    render();
  };

  const typing = () => {
    const active = document.activeElement;
    return active && active.tagName === 'TEXTAREA';
  };

  const card = (m) => {
    const node = el('div', { class: 'card' + (m.unread ? ' unread' : '') });
    node.append(el('div', { class: 'meta' },
      m.kind + ' from ' + m.from + ' at ' + m.time + (m.unread ? ' (unread)' : '')));
    node.append(el('div', { class: 'body' }, m.body));
    if (m.attachments.length) {
      node.append(el('div', { class: 'meta' }, 'attachments: ' + m.attachments.join(', ')));
    }
    for (const r of m.replies) {
      const reply = el('div', { class: 'reply' });
      reply.append(el('div', { class: 'meta' }, 'you, ' + r.kind + ' at ' + r.time));
      reply.append(el('div', { class: 'body' }, r.body));
      node.append(reply);
    }
    if (m.answerable) {
      const area = el('textarea', { placeholder: 'Answer ' + m.from });
      area.value = state.drafts[m.id] || '';
      area.addEventListener('input', () => { state.drafts[m.id] = area.value; });
      const select = el('select');
      for (const kind of m.reply_kinds) select.append(el('option', { value: kind }, kind));
      select.value = state.kinds[m.id] || m.reply_kinds[0];
      select.addEventListener('change', () => { state.kinds[m.id] = select.value; });
      const send = el('button', {}, 'Send');
      send.addEventListener('click', () => act(
        '/api/reply',
        { msg_id: m.id, kind: select.value, body: area.value },
        () => { delete state.drafts[m.id]; },
      ));
      node.append(el('div', { class: 'row' }, area), el('div', { class: 'row' }, select, send));
    }
    return node;
  };

  const render = () => {
    if (state.data && typing()) {
      // Do not rebuild under the owner's cursor; only the error line is refreshed.
      const line = document.getElementById('error');
      if (line) line.textContent = state.error;
      return;
    }
    const data = state.data;
    root.replaceChildren();
    root.append(el('h1', {}, data ? 'agent-bus: ' + data.me : 'agent-bus'));
    const err = el('div', { class: 'error', id: 'error' }, state.error);
    root.append(err);
    if (!data) return;

    const area = el('textarea', { placeholder: 'New message to ' + data.peer });
    area.value = state.newBody;
    area.addEventListener('input', () => { state.newBody = area.value; });
    const send = el('button', {}, 'Send');
    send.addEventListener('click', () => act(
      '/api/send', { body: area.value }, () => { state.newBody = ''; },
    ));
    root.append(el('div', { class: 'card' },
      el('div', { class: 'meta' }, 'New message to ' + data.peer),
      area, el('div', { class: 'row' }, send)));

    const unread = data.messages.filter((m) => m.unread).map((m) => m.id);
    const ack = el('button', {}, 'Ack shown (' + unread.length + ')');
    ack.disabled = unread.length === 0;
    ack.addEventListener('click', () => act('/api/ack', { msg_ids: unread }));
    root.append(el('div', { class: 'bar' }, ack));

    if (!data.messages.length) root.append(el('div', { class: 'meta' }, 'No messages.'));
    for (const m of data.messages) root.append(card(m));
  };

  load();
  setInterval(load, 5000);
})();
"""
