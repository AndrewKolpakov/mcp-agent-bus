"""The hub Lambda: authorizers, HTTP routes and WebSocket routes in one function.

`Hub` holds the rules and works on plain values; `handler` only translates
API Gateway's event shapes to and from it. The caller's agent name always
comes from the authorizer, never from a request body.

What `POST /events` accepts, by event type:

| type         | the hub sets      | allowed when                                         |
| ------------ | ----------------- | ---------------------------------------------------- |
| `message`    | `from`, `thread`  | `to` shares a team with the caller; `reply_to`, if   |
|              |                   | any, is a message the caller is a party to and its   |
|              |                   | kind allows the reply; the pair is under the loop    |
|              |                   | limit unless kind is `await_human`                   |
| `ack`        | `by`              | the message is addressed to the caller               |
| `delivered`  | `to`              | the message is addressed to the caller               |
| `human_turn` | `agent`           | always                                               |

Every event also gets `ts` and `seq` from the hub. A message id that is
already taken returns the stored message when the caller sent it, and 409
otherwise; it is never pushed twice. Acks, deliveries and human turns have
no id of their own, and a repeat of one is harmless: replay treats them as
sets. An unknown agent and an agent on no shared team get the same refusal,
and so do an unknown message and someone else's, so neither leaks.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import cache
from typing import Any

from pydantic import ValidationError

from agent_bus.bus import MAX_BODY_BYTES, BusError, RateLimitError, check_rate, check_reply
from agent_bus.config import DEFAULT_RATE_LIMIT, DEFAULT_RATE_WINDOW_SECONDS
from agent_bus.hub.push import ApiGatewayPusher, Pusher
from agent_bus.hub.table import Claim, DuplicateError, MessageRef, Table
from agent_bus.models import (
    KINDS,
    AckEvent,
    DeliveredEvent,
    Event,
    HumanTurnEvent,
    MessageEvent,
    check_agent_name,
    new_id,
    participants,
)
from agent_bus.state import BusState

log = logging.getLogger()
log.setLevel(logging.INFO)

SUBPROTOCOL = "agent-bus"
LIVE_SECONDS = 150.0
"""A claim is live while its connection pinged this recently; clients ping every 60 s."""
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
"""Below Lambda's 6 MB response limit, with room for the envelope."""
RECENT_CAP = 1000
"""Events read back for the loop window; far above any legitimate pace."""

MESSAGE_ID = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")
SESSION_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


class HubError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass(frozen=True)
class Reply:
    status: int
    body: Any
    jsonl: bool = False


def _record(event: Event) -> dict[str, Any]:
    record: dict[str, Any] = json.loads(event.model_dump_json(by_alias=True, exclude_none=True))
    return record


def _text(body: dict[str, Any], name: str, *, required: bool = True) -> str | None:
    value = body.get(name)
    if value is None and not required:
        return None
    if not isinstance(value, str):
        raise HubError(400, f"{name} must be a string")
    return value


def _session(body: dict[str, Any]) -> str:
    value = _text(body, "session") or ""
    if not SESSION_ID.fullmatch(value):
        raise HubError(400, "session must be 1-128 of A-Z, a-z, 0-9, '_', '.', ':' or '-'")
    return value


class Hub:
    def __init__(
        self,
        table: Table,
        pusher: Pusher,
        *,
        rate_limit: int = DEFAULT_RATE_LIMIT,
        rate_window: float = DEFAULT_RATE_WINDOW_SECONDS,
        clock: Callable[[], float] = time.time,
        ws_url: str = "",
    ) -> None:
        self.ws_url = ws_url
        self.table = table
        self.pusher = pusher
        self.rate_limit = rate_limit
        self.rate_window = rate_window
        self.clock = clock

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.clock(), UTC)

    def hello(self, agent: str) -> Reply:
        """Who the token belongs to and where the WebSocket is: all a client needs to start."""
        return Reply(200, {"agent": agent, "ws": self.ws_url})

    # --- events -------------------------------------------------------------

    def post_event(self, agent: str, body: dict[str, Any]) -> Reply:
        kind = body.get("type", "message")
        if kind == "message":
            return self._message(agent, body)
        if kind in ("ack", "delivered"):
            return self._settle(agent, kind, body)
        if kind == "human_turn":
            event = HumanTurnEvent(agent=agent, session=_session(body), ts=self.now())
            return self._write(event, participants(event))
        raise HubError(400, f"unsupported event type: {kind!r}")

    def _message(self, agent: str, body: dict[str, Any]) -> Reply:
        to = _text(body, "to") or ""
        text = _text(body, "body") or ""
        kind = body.get("kind", "text")
        reply_to = _text(body, "reply_to", required=False)
        msg_id = _text(body, "id", required=False) or new_id()
        if kind not in KINDS:
            raise HubError(400, f"kind must be one of {', '.join(KINDS)}")
        if not MESSAGE_ID.fullmatch(msg_id):
            raise HubError(400, "id must be a ULID")
        try:
            check_agent_name(to)
        except ValueError as exc:
            raise HubError(400, str(exc)) from exc
        if to == agent:
            raise HubError(400, "cannot send a message to yourself")
        if not text.strip():
            raise HubError(400, "message body is empty")
        if len(text.encode()) > MAX_BODY_BYTES:
            raise HubError(400, f"message body exceeds {MAX_BODY_BYTES} bytes")

        taken = self.table.message(msg_id)
        if taken is not None:
            return self._duplicate(agent, taken)
        if to not in self.table.teammates(agent):
            raise HubError(403, f"{to} is not on any of your teams")

        thread = msg_id
        if reply_to is not None:
            original = self._visible(
                agent, reply_to, f"unknown message id for reply_to: {reply_to}"
            )
            try:
                check_reply(original, kind)
            except BusError as exc:
                raise HubError(422, str(exc)) from exc
            thread = original.thread

        if kind != "await_human":
            since = self.now() - timedelta(seconds=self.rate_window)
            window = BusState.from_events(self.table.recent_view(agent, since, RECENT_CAP))
            try:
                check_rate(
                    window.messages_between(agent, to, since=since),
                    agent,
                    to,
                    limit=self.rate_limit,
                    window=self.rate_window,
                )
            except RateLimitError as exc:
                raise HubError(429, str(exc)) from exc

        event = MessageEvent(
            id=msg_id,
            sender=agent,
            to=to,
            kind=kind,
            body=text,
            reply_to=reply_to,
            thread=thread,
            ts=self.now(),
        )
        try:
            return self._write(event, participants(event))
        except DuplicateError as dup:
            return self._duplicate(agent, dup.ref)

    def _duplicate(self, agent: str, ref: MessageRef) -> Reply:
        if ref.sender != agent:
            raise HubError(409, "message id is already in use")
        return Reply(200, _record(self.table.event_at(ref.seq)))

    def _visible(self, agent: str, msg_id: str, refusal: str) -> MessageRef:
        ref = self.table.message(msg_id) if MESSAGE_ID.fullmatch(msg_id) else None
        if ref is None or agent not in (ref.sender, ref.to):
            raise HubError(404, refusal)
        return ref

    def _settle(self, agent: str, kind: str, body: dict[str, Any]) -> Reply:
        msg_id = _text(body, "id") or ""
        ref = self._visible(agent, msg_id, f"unknown message id: {msg_id}")
        if ref.to != agent:
            raise HubError(403, f"message {msg_id} is addressed to {ref.to}, not {agent}")
        event: AckEvent | DeliveredEvent
        if kind == "ack":
            event = AckEvent(id=msg_id, by=agent, ts=self.now())
        else:
            event = DeliveredEvent(id=msg_id, to=agent, session=_session(body), ts=self.now())
        return self._write(event, participants(event, ref))

    def _write(self, event: Event, parties: Iterable[str]) -> Reply:
        views = sorted(parties)
        stored = self.table.append(event, views)
        assert stored.seq is not None
        log.info("seq=%s type=%s views=%s", stored.seq, stored.type, ",".join(views))
        self.push(views, stored.seq)
        return Reply(200, _record(stored))

    def push(self, agents: Iterable[str], seq: int) -> None:
        for agent in agents:
            for conn in self.table.connections(agent):
                if not self.pusher.post(conn.endpoint, conn.id, {"seq": seq}):
                    log.info("connection %s of %s is gone", conn.id, agent)
                    self.table.forget_connection(agent, conn.id)

    def events(self, agent: str, query: dict[str, str]) -> Reply:
        try:
            after = int(query.get("after", "0"))
            limit = int(query.get("limit", str(DEFAULT_LIMIT)))
        except ValueError as exc:
            raise HubError(400, "after and limit must be integers") from exc
        if after < 0 or not 1 <= limit <= MAX_LIMIT:
            raise HubError(400, f"after must be >= 0 and limit 1..{MAX_LIMIT}")
        lines = self.table.view(agent, after, limit, MAX_RESPONSE_BYTES)
        return Reply(200, "".join(line + "\n" for line in lines), jsonl=True)

    # --- peers and the channel claim ----------------------------------------

    def live(self, agent: str) -> bool:
        claim = self.table.claim_of(agent)
        return claim is not None and self._connection_live(agent, claim.connection)

    def _connection_live(self, agent: str, conn: str) -> bool:
        found = self.table.connection(agent, conn)
        return found is not None and self.clock() - found.last_ping < LIVE_SECONDS

    def peers(self, agent: str) -> Reply:
        names = sorted(self.table.teammates(agent))
        return Reply(200, [{"name": name, "live": self.live(name)} for name in names])

    def get_session(self, agent: str) -> Reply:
        claim = self.table.claim_of(agent)
        if claim is None:
            raise HubError(404, "no session holds the channel")
        live = self._connection_live(agent, claim.connection)
        return Reply(200, {"session": claim.session, "connection": claim.connection, "live": live})

    def put_session(self, agent: str, body: dict[str, Any]) -> Reply:
        """Claim the channel; repeating it for the same session keeps the claim."""
        session = _session(body)
        conn = _text(body, "connection") or ""
        if self.table.connection(agent, conn) is None:
            raise HubError(400, f"connection {conn!r} is not one of yours; ping to learn its id")
        current = self.table.claim_of(agent)
        if (
            current is not None
            and current.session != session
            and self._connection_live(agent, current.connection)
        ):
            raise HubError(409, "another live session holds the channel")
        claim = Claim(session=session, connection=conn)
        if not self.table.put_claim(agent, claim, current, self.clock()):
            raise HubError(409, "another session claimed the channel at the same time")
        log.info("agent=%s session=%s holds the channel", agent, session)
        return Reply(200, {"session": session, "connection": conn})

    def delete_session(self, agent: str, body: dict[str, Any]) -> Reply:
        session = _session(body)
        return Reply(200, {"released": self.table.delete_claim(agent, session)})

    # --- WebSocket ----------------------------------------------------------

    def connect(self, agent: str, conn: str, endpoint: str) -> None:
        self.table.add_connection(agent, conn, endpoint, self.clock())
        log.info("connect %s agent=%s", conn, agent)

    def disconnect(self, conn: str) -> None:
        agent = self.table.connection_agent(conn)
        if agent is not None:
            self.table.forget_connection(agent, conn)
        log.info("disconnect %s agent=%s", conn, agent)

    def ping(self, conn: str, endpoint: str) -> None:
        """Record liveness; the pong tells the client its own connection id."""
        agent = self.table.connection_agent(conn)
        if agent is not None and self.table.touch_connection(agent, conn, self.clock()):
            self.pusher.post(endpoint, conn, {"type": "pong", "connection": conn})
        else:
            self.pusher.post(endpoint, conn, {"type": "error", "error": "unknown connection"})


# --- API Gateway adapter -------------------------------------------------------


@cache
def _hub() -> Hub:
    return Hub(
        Table(os.environ["TABLE"]),
        ApiGatewayPusher(),
        rate_limit=int(os.environ.get("RATE_LIMIT", DEFAULT_RATE_LIMIT)),
        rate_window=float(os.environ.get("RATE_WINDOW_SECONDS", DEFAULT_RATE_WINDOW_SECONDS)),
        ws_url=os.environ.get("WS_URL", ""),
    )


def _headers(event: dict[str, Any]) -> dict[str, str]:
    return {k.lower(): v for k, v in (event.get("headers") or {}).items()}


def http_authorizer(event: dict[str, Any], hub: Hub) -> dict[str, Any]:
    """HTTP API REQUEST authorizer, payload 2.0 with simple responses."""
    token = _headers(event).get("authorization", "").removeprefix("Bearer ").strip()
    agent = hub.table.agent_for_token(token) if token else None
    return {"isAuthorized": agent is not None, "context": {"agent": agent or ""}}


def ws_authorizer(event: dict[str, Any], hub: Hub) -> dict[str, Any]:
    """`$connect` authorizer: the token rides in `Sec-WebSocket-Protocol: agent-bus, <token>`."""
    offered = [p.strip() for p in _headers(event).get("sec-websocket-protocol", "").split(",")]
    agent = None
    if len(offered) == 2 and offered[0] == SUBPROTOCOL and offered[1]:
        agent = hub.table.agent_for_token(offered[1])
    if agent is None:
        log.info("websocket connect refused")
        raise PermissionError("Unauthorized")  # API Gateway answers 401
    return {
        "principalId": agent,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {"Action": "execute-api:Invoke", "Effect": "Allow", "Resource": event["methodArn"]}
            ],
        },
        "context": {"agent": agent},
    }


def _response(reply: Reply) -> dict[str, Any]:
    return {
        "statusCode": reply.status,
        "headers": {"content-type": "application/x-ndjson" if reply.jsonl else "application/json"},
        "body": reply.body if reply.jsonl else json.dumps(reply.body),
    }


def _json_body(event: dict[str, Any]) -> dict[str, Any]:
    raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode()
    try:
        body = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HubError(400, "request body is not JSON") from exc
    if not isinstance(body, dict):
        raise HubError(400, "request body must be a JSON object")
    return body


def http(event: dict[str, Any], hub: Hub) -> dict[str, Any]:
    ctx = event["requestContext"]
    agent = ctx["authorizer"]["lambda"]["agent"]
    route = (ctx["http"]["method"], ctx["http"]["path"])
    try:
        if route == ("GET", "/hello"):
            reply = hub.hello(agent)
        elif route == ("POST", "/events"):
            reply = hub.post_event(agent, _json_body(event))
        elif route == ("GET", "/events"):
            reply = hub.events(agent, event.get("queryStringParameters") or {})
        elif route == ("GET", "/peers"):
            reply = hub.peers(agent)
        elif route == ("GET", "/session"):
            reply = hub.get_session(agent)
        elif route == ("PUT", "/session"):
            reply = hub.put_session(agent, _json_body(event))
        elif route == ("DELETE", "/session"):
            reply = hub.delete_session(agent, _json_body(event))
        else:
            raise HubError(404, f"no route {route[0]} {route[1]}")
    except HubError as exc:
        log.info("agent=%s %s %s -> %s", agent, *route, exc.status)
        reply = Reply(exc.status, {"error": exc.message})
    except ValidationError as exc:
        reply = Reply(400, {"error": str(exc)})
    return _response(reply)


def websocket(event: dict[str, Any], hub: Hub) -> dict[str, Any]:
    ctx = event["requestContext"]
    conn = ctx["connectionId"]
    route = ctx["routeKey"]
    endpoint = f"https://{ctx['domainName']}/{ctx['stage']}"
    if route == "$connect":
        hub.connect(ctx["authorizer"]["agent"], conn, endpoint)
        return {"statusCode": 200, "headers": {"Sec-WebSocket-Protocol": SUBPROTOCOL}}
    if route == "$disconnect":
        hub.disconnect(conn)
    elif route == "ping":
        hub.ping(conn, endpoint)
    else:
        hub.pusher.post(endpoint, conn, {"type": "error", "error": f"unknown action {route}"})
    return {"statusCode": 200}


def handler(event: dict[str, Any], context: Any, hub: Hub | None = None) -> dict[str, Any]:
    hub = hub or _hub()
    if event.get("type") == "REQUEST":
        if "methodArn" in event:
            return ws_authorizer(event, hub)
        return http_authorizer(event, hub)
    if "eventType" in event.get("requestContext", {}):
        return websocket(event, hub)
    return http(event, hub)
