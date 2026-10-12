"""The hub against a moto DynamoDB table, with a recording pusher in place of API Gateway."""

from __future__ import annotations

import json
from typing import Any

import pytest
from hubfakes import ENDPOINT, Clock, RecordingPusher, clock, hub, pusher, table, tokens

__all__ = ["clock", "hub", "pusher", "table", "tokens"]  # fixtures

from agent_bus.hub import app
from agent_bus.hub.app import Hub, HubError, Reply
from agent_bus.hub.table import Claim, MessageRef, Table
from agent_bus.models import EVENT_ADAPTER, MessageEvent, new_id


def send(hub: Hub, sender: str, to: str, body: str = "hi", **extra: Any) -> dict[str, Any]:
    reply = hub.post_event(sender, {"to": to, "body": body, **extra})
    assert reply.status == 200
    record: dict[str, Any] = reply.body
    return record


def refused(call: Any, status: int, match: str) -> None:
    with pytest.raises(HubError, match=match) as info:
        call()
    assert info.value.status == status


def view(hub: Hub, agent: str, after: int = 0) -> list[dict[str, Any]]:
    reply = hub.events(agent, {"after": str(after)})
    return [json.loads(line) for line in reply.body.splitlines()]


# --- messages -----------------------------------------------------------------


def test_message_to_a_teammate_lands_in_both_views_with_hub_fields(hub: Hub) -> None:
    record = send(hub, "alice", "bob", "hello", **{"from": "bob", "seq": 99, "ts": "2000-01-01"})
    assert record["from"] == "alice"
    assert record["seq"] == 1
    assert record["thread"] == record["id"]
    assert record["ts"].startswith("2027-01-15")
    assert view(hub, "alice") == view(hub, "bob") == [record]
    assert view(hub, "carol") == []


def test_seq_counts_up_across_senders(hub: Hub) -> None:
    seqs = [send(hub, "alice", "bob")["seq"], send(hub, "carol", "dave")["seq"]]
    seqs.append(send(hub, "dave", "alice")["seq"])
    assert seqs == [1, 2, 3]
    assert [e["seq"] for e in view(hub, "dave")] == [2, 3]


@pytest.mark.parametrize("to", ["carol", "nobody"])
def test_only_teammates_can_be_written_to(hub: Hub, to: str) -> None:
    refused(lambda: send(hub, "alice", to), 403, f"{to} is not on any of your teams")


@pytest.mark.parametrize(
    ("body", "match"),
    [
        ({"to": "alice", "body": "x"}, "yourself"),
        ({"to": "bob", "body": "  "}, "empty"),
        ({"to": "bob", "body": "x" * (64 * 1024 + 1)}, "exceeds"),
        ({"to": "bob", "body": "x", "kind": "shout"}, "kind must be"),
        ({"to": "bob", "body": "x", "id": "../MSG"}, "ULID"),
        ({"to": "Bob!", "body": "x"}, "invalid agent name"),
        ({"to": "bob"}, "body must be a string"),
        ({"type": "session_ready", "agent": "alice", "session": "s"}, "unsupported"),
    ],
)
def test_malformed_requests_are_refused(hub: Hub, body: dict[str, Any], match: str) -> None:
    refused(lambda: hub.post_event("alice", body), 400, match)


def test_a_reply_inherits_the_thread(hub: Hub) -> None:
    first = send(hub, "alice", "bob", kind="task")
    reply = send(hub, "bob", "alice", kind="result", reply_to=first["id"])
    assert reply["thread"] == first["id"]


def test_replying_to_someone_elses_message_looks_like_an_unknown_one(hub: Hub) -> None:
    theirs = send(hub, "alice", "bob")
    refused(lambda: send(hub, "dave", "bob", reply_to=theirs["id"]), 404, "unknown message id")
    refused(lambda: send(hub, "dave", "bob", reply_to=new_id()), 404, "unknown message id")


def test_the_chain_rule_holds_on_the_hub(hub: Hub) -> None:
    result = send(hub, "alice", "bob", kind="result")
    refused(lambda: send(hub, "bob", "alice", reply_to=result["id"]), 422, "ends its chain")
    send(hub, "bob", "alice", kind="task", reply_to=result["id"])


def test_loop_limit_counts_the_pair_and_spares_await_human(hub: Hub, clock: Clock) -> None:
    send(hub, "alice", "bob")
    send(hub, "bob", "alice")
    send(hub, "alice", "dave")
    send(hub, "alice", "bob")
    refused(lambda: send(hub, "bob", "alice"), 429, "kind='await_human'")
    send(hub, "bob", "alice", kind="await_human")
    clock.t += 301
    send(hub, "bob", "alice")


def test_a_repeated_id_returns_the_stored_message_once(hub: Hub, pusher: RecordingPusher) -> None:
    hub.connect("bob", "c-bob", ENDPOINT)
    msg_id = new_id()
    first = send(hub, "alice", "bob", "once", id=msg_id)
    again = send(hub, "alice", "bob", "changed", id=msg_id)
    assert again == first
    assert pusher.seqs("c-bob") == [first["seq"]]
    refused(lambda: send(hub, "dave", "bob", id=msg_id), 409, "already in use")
    assert len(view(hub, "bob")) == 1


def test_a_duplicate_found_inside_the_transaction_is_handled(
    hub: Hub, table: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    msg_id = new_id()
    first = send(hub, "alice", "bob", id=msg_id)
    real = table.message

    def miss_once(i: str, missed: list[bool] = []) -> MessageRef | None:  # noqa: B006
        # The pre-check misses the row, as when two retries race.
        if not missed:
            missed.append(True)
            return None
        return real(i)

    monkeypatch.setattr(table, "message", miss_once)
    assert send(hub, "alice", "bob", id=msg_id) == first
    assert len(view(hub, "bob")) == 1


def test_the_sequencer_retries_past_a_taken_seq(
    hub: Hub, table: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    send(hub, "alice", "bob")
    stale = [0]
    real = table.last_seq
    attempts: list[int] = []

    def last_seq() -> int:
        attempts.append(1)
        return stale.pop() if stale else real()

    monkeypatch.setattr(table, "last_seq", last_seq)
    assert send(hub, "alice", "bob")["seq"] == 2
    assert len(attempts) == 2


def test_a_removed_member_keeps_history_but_cannot_write(hub: Hub, table: Table) -> None:
    send(hub, "alice", "bob")
    table.remove_member("core", "bob")
    refused(lambda: send(hub, "alice", "bob"), 403, "not on any of your teams")
    assert len(view(hub, "bob")) == 1


# --- acks, deliveries, human turns --------------------------------------------


def test_the_addressee_acks_and_both_parties_see_it(hub: Hub) -> None:
    msg = send(hub, "alice", "bob")
    ack = hub.post_event("bob", {"type": "ack", "id": msg["id"], "by": "alice"}).body
    assert ack["by"] == "bob"
    assert view(hub, "alice", after=msg["seq"]) == [ack]
    assert view(hub, "bob", after=msg["seq"]) == [ack]


def test_only_the_addressee_settles_a_message(hub: Hub) -> None:
    msg = send(hub, "alice", "bob")
    refused(
        lambda: hub.post_event("alice", {"type": "ack", "id": msg["id"]}), 403, "addressed to bob"
    )
    refused(lambda: hub.post_event("dave", {"type": "ack", "id": msg["id"]}), 404, "unknown")
    delivered = {"type": "delivered", "id": msg["id"], "session": "S1"}
    refused(lambda: hub.post_event("alice", delivered), 403, "addressed to bob")
    stored = hub.post_event("bob", delivered).body
    assert stored["to"] == "bob"
    assert stored["session"] == "S1"


def test_a_human_turn_is_seen_only_by_its_agent(hub: Hub) -> None:
    turn = hub.post_event("bob", {"type": "human_turn", "session": "claude-1", "agent": "x"}).body
    assert turn["agent"] == "bob"
    assert view(hub, "bob") == [turn]
    assert view(hub, "alice") == []
    refused(lambda: hub.post_event("bob", {"type": "human_turn", "session": "a b"}), 400, "session")


# --- reading ------------------------------------------------------------------


def test_events_page_by_after_and_limit(hub: Hub) -> None:
    hub.table.add_connection("bob", "c1", ENDPOINT, 0)
    hub.table.put_claim("bob", Claim("s", "c1"), None, 0)
    seqs = [send(hub, "alice", "bob")["seq"] for _ in range(3)]
    page = hub.events("bob", {"after": "0", "limit": "2"}).body.splitlines()
    assert [EVENT_ADAPTER.validate_json(line).seq for line in page] == seqs[:2]
    assert [e["seq"] for e in view(hub, "bob", after=seqs[1])] == seqs[2:]
    assert view(hub, "bob", after=seqs[2]) == []
    refused(lambda: hub.events("bob", {"limit": "0"}), 400, "limit")
    refused(lambda: hub.events("bob", {"after": "x"}), 400, "integers")


def test_view_stops_at_the_byte_cap_but_returns_at_least_one(hub: Hub, table: Table) -> None:
    for _ in range(3):
        send(hub, "alice", "bob", "x" * 1000)
    assert len(table.view("bob", 0, 100, max_bytes=2500)) == 2
    assert len(table.view("bob", 0, 100, max_bytes=10)) == 1


# --- peers and the channel claim ------------------------------------------------


def test_peers_are_teammates_with_liveness(hub: Hub, clock: Clock) -> None:
    assert hub.peers("alice").body == [
        {"name": "bob", "live": False},
        {"name": "dave", "live": False},
    ]
    assert [p["name"] for p in hub.peers("dave").body] == ["alice", "bob", "carol"]
    hub.connect("bob", "c-bob", ENDPOINT)
    hub.put_session("bob", {"session": "s1", "connection": "c-bob"})
    assert hub.peers("alice").body[0] == {"name": "bob", "live": True}
    clock.t += 151
    assert hub.peers("alice").body[0] == {"name": "bob", "live": False}


def test_one_live_session_holds_the_channel(hub: Hub, clock: Clock) -> None:
    hub.connect("bob", "c1", ENDPOINT)
    hub.connect("bob", "c2", ENDPOINT)
    hub.connect("alice", "c-alice", ENDPOINT)
    refused(
        lambda: hub.put_session("bob", {"session": "s1", "connection": "c-alice"}),
        400,
        "not one of yours",
    )
    hub.put_session("bob", {"session": "s1", "connection": "c1"})
    refused(lambda: hub.put_session("bob", {"session": "s2", "connection": "c2"}), 409, "live")
    assert hub.put_session("bob", {"session": "s1", "connection": "c1"}).status == 200
    assert hub.get_session("bob").body == {"session": "s1", "connection": "c1", "live": True}
    clock.t += 100
    hub.ping("c2", ENDPOINT)
    clock.t += 60
    # c1 has not pinged for 160 s, so s2 takes over.
    hub.put_session("bob", {"session": "s2", "connection": "c2"})
    assert hub.delete_session("bob", {"session": "s1"}).body == {"released": False}
    assert hub.delete_session("bob", {"session": "s2"}).body == {"released": True}
    refused(lambda: hub.get_session("bob"), 404, "no session")


def test_concurrent_claims_cannot_both_win(hub: Hub, table: Table) -> None:
    hub.connect("bob", "c1", ENDPOINT)
    assert table.put_claim("bob", Claim("s1", "c1"), None, 0)
    assert not table.put_claim("bob", Claim("s2", "c1"), None, 0)
    assert not table.put_claim("bob", Claim("s2", "c1"), Claim("s0", "c0"), 0)


# --- WebSocket ----------------------------------------------------------------


def test_events_are_pushed_to_every_connection_of_every_party(
    hub: Hub, pusher: RecordingPusher
) -> None:
    for agent, conn in (("alice", "a1"), ("bob", "b1"), ("bob", "b2"), ("dave", "d1")):
        hub.connect(agent, conn, ENDPOINT)
    msg = send(hub, "alice", "bob")
    assert pusher.seqs("a1") == pusher.seqs("b1") == pusher.seqs("b2") == [msg["seq"]]
    assert pusher.seqs("d1") == []


def test_a_gone_connection_is_forgotten(hub: Hub, pusher: RecordingPusher, table: Table) -> None:
    hub.connect("bob", "b1", ENDPOINT)
    pusher.gone.add("b1")
    send(hub, "alice", "bob")
    assert table.connections("bob") == []
    assert table.connection_agent("b1") is None


def test_ping_answers_with_the_connection_id_and_refreshes_it(
    hub: Hub, pusher: RecordingPusher, table: Table, clock: Clock
) -> None:
    hub.connect("bob", "b1", ENDPOINT)
    clock.t += 30
    hub.ping("b1", ENDPOINT)
    assert pusher.posts[-1] == ("b1", {"type": "pong", "connection": "b1"})
    found = table.connection("bob", "b1")
    assert found is not None
    assert found.last_ping == clock.t
    hub.disconnect("b1")
    hub.ping("b1", ENDPOINT)
    assert pusher.posts[-1] == ("b1", {"type": "error", "error": "unknown connection"})


# --- the API Gateway adapter --------------------------------------------------


def http_event(method: str, path: str, agent: str, body: Any = None, **query: str) -> Any:
    return {
        "version": "2.0",
        "requestContext": {
            "http": {"method": method, "path": path},
            "authorizer": {"lambda": {"agent": agent}},
        },
        "queryStringParameters": query or None,
        "body": None if body is None else body if isinstance(body, str) else json.dumps(body),
        "isBase64Encoded": False,
    }


def ws_event(route: str, conn: str, **extra: Any) -> Any:
    ctx = {
        "routeKey": route,
        "eventType": route.strip("$").upper(),
        "connectionId": conn,
        "domainName": "ws.example",
        "stage": "dev",
        **extra,
    }
    return {"requestContext": ctx}


def test_http_routes_through_the_adapter(hub: Hub) -> None:
    posted = app.handler(
        http_event("POST", "/events", "alice", {"to": "bob", "body": "x"}), None, hub
    )
    assert posted["statusCode"] == 200
    seq = json.loads(posted["body"])["seq"]
    got = app.handler(http_event("GET", "/events", "bob", after="0"), None, hub)
    assert got["headers"]["content-type"] == "application/x-ndjson"
    assert MessageEvent.model_validate_json(got["body"].strip()).seq == seq
    refusal = app.handler(
        http_event("POST", "/events", "alice", {"to": "carol", "body": "x"}), None, hub
    )
    assert refusal["statusCode"] == 403
    assert "not on any of your teams" in json.loads(refusal["body"])["error"]
    assert (
        app.handler(http_event("POST", "/events", "alice", "not json"), None, hub)["statusCode"]
        == 400
    )
    assert app.handler(http_event("GET", "/nope", "alice"), None, hub)["statusCode"] == 404
    assert json.loads(app.handler(http_event("GET", "/peers", "carol"), None, hub)["body"]) == [
        {"name": "dave", "live": False}
    ]


def test_hello_names_the_agent_and_the_websocket(table: Table, tokens: dict[str, str]) -> None:
    hub = Hub(table, RecordingPusher(), ws_url="wss://ws.example/bus")
    got = app.handler(http_event("GET", "/hello", "bob"), None, hub)
    assert json.loads(got["body"]) == {"agent": "bob", "ws": "wss://ws.example/bus"}


def test_authorizers_resolve_tokens(hub: Hub, tokens: dict[str, str]) -> None:
    def http_auth(header: str) -> Reply:
        out = app.handler(
            {"type": "REQUEST", "routeArn": "arn", "headers": {"authorization": header}}, None, hub
        )
        return Reply(200, out)

    assert http_auth(f"Bearer {tokens['alice']}").body == {
        "isAuthorized": True,
        "context": {"agent": "alice"},
    }
    assert http_auth("Bearer ab_wrong").body["isAuthorized"] is False
    assert http_auth("").body["isAuthorized"] is False

    def ws_auth(protocol: str) -> dict[str, Any]:
        event = {
            "type": "REQUEST",
            "methodArn": "arn:ws",
            "headers": {"Sec-WebSocket-Protocol": protocol},
        }
        result: dict[str, Any] = app.handler(event, None, hub)
        return result

    allowed = ws_auth(f"agent-bus, {tokens['bob']}")
    assert allowed["context"] == {"agent": "bob"}
    assert allowed["policyDocument"]["Statement"][0]["Resource"] == "arn:ws"
    for bad in ("agent-bus, ab_wrong", f"other, {tokens['bob']}", tokens["bob"], ""):
        with pytest.raises(PermissionError, match="Unauthorized"):
            ws_auth(bad)


def test_websocket_routes_through_the_adapter(
    hub: Hub, pusher: RecordingPusher, table: Table
) -> None:
    connected = app.handler(ws_event("$connect", "b1", authorizer={"agent": "bob"}), None, hub)
    assert connected["headers"] == {"Sec-WebSocket-Protocol": "agent-bus"}
    assert table.connection_agent("b1") == "bob"
    app.handler(ws_event("ping", "b1"), None, hub)
    assert pusher.posts[-1] == ("b1", {"type": "pong", "connection": "b1"})
    app.handler(ws_event("$default", "b1"), None, hub)
    assert pusher.posts[-1][1]["type"] == "error"
    app.handler(ws_event("$disconnect", "b1"), None, hub)
    assert table.connection_agent("b1") is None
