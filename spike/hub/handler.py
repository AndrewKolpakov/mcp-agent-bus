"""Stage 7 spike: the whole hub in one Lambda, routed by event shape.

Not production code. It exists to answer Q7-Q11 in PLAN.md and is deleted
once the real hub (stage 9) repeats its checks.

Event shapes this function receives:
- HTTP API Lambda authorizer, payload 2.0 (`type == "REQUEST"`, `routeArn`)
- WebSocket `$connect` authorizer (`type == "REQUEST"`, `methodArn`)
- HTTP API request, payload 2.0 (`requestContext.http`)
- WebSocket route (`requestContext.eventType`)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

# Q10: the pass criterion is that this import works inside Lambda.
from agent_bus.models import MessageEvent, now

log = logging.getLogger()
log.setLevel(logging.INFO)

TABLE = os.environ["TABLE"]
ddb = boto3.client("dynamodb")
_mgmt: dict[str, Any] = {}

TICKET_SECONDS = 60
CONN_SECONDS = 7200
MAX_ATTEMPTS = 50
COLD = True


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def mgmt(endpoint: str) -> Any:
    if endpoint not in _mgmt:
        _mgmt[endpoint] = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint)
    return _mgmt[endpoint]


def seq_key(seq: int) -> str:
    return f"{seq:012d}"


# --- identity -----------------------------------------------------------------


def agent_for_token(token: str) -> str | None:
    item = ddb.get_item(
        TableName=TABLE, Key={"pk": {"S": f"TOKEN#{token_hash(token)}"}, "sk": {"S": "AGENT"}}
    ).get("Item")
    return item["agent"]["S"] if item else None


def agent_for_ticket(ticket: str) -> str | None:
    """Single use: the conditional delete is what consumes it."""
    try:
        old = ddb.delete_item(
            TableName=TABLE,
            Key={"pk": {"S": f"TICKET#{ticket}"}, "sk": {"S": "AGENT"}},
            ConditionExpression="attribute_exists(pk) AND expires > :now",
            ExpressionAttributeValues={":now": {"N": str(int(time.time()))}},
            ReturnValues="ALL_OLD",
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return None
        raise
    return old["Attributes"]["agent"]["S"]


def resolve(credential: str) -> tuple[str | None, str]:
    if credential.startswith("tkt_"):
        return agent_for_ticket(credential), "ticket"
    return agent_for_token(credential), "token"


def http_authorizer(event: dict[str, Any]) -> dict[str, Any]:
    header = (event.get("headers") or {}).get("authorization", "")
    token = header.removeprefix("Bearer ").strip()
    agent = agent_for_token(token) if token else None
    log.info("http authorizer: agent=%s", agent)
    return {"isAuthorized": agent is not None, "context": {"agent": agent or ""}}


def ws_authorizer(event: dict[str, Any]) -> dict[str, Any]:
    query = event.get("queryStringParameters") or {}
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    credential, source = "", ""
    if query.get("token"):
        credential, source = query["token"], "query"
    elif query.get("ticket"):
        credential, source = query["ticket"], "query"
    elif headers.get("sec-websocket-protocol"):
        # "agent-bus, <credential>": the first entry is echoed back on $connect.
        parts = [p.strip() for p in headers["sec-websocket-protocol"].split(",")]
        if len(parts) == 2 and parts[0] == "agent-bus":
            credential, source = parts[1], "subprotocol"
    agent, kind = resolve(credential) if credential else (None, "")
    log.info("ws authorizer: source=%s kind=%s agent=%s", source, kind, agent)
    if agent is None:
        raise Exception("Unauthorized")  # API Gateway turns this into 401
    return {
        "principalId": agent,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {"Action": "execute-api:Invoke", "Effect": "Allow", "Resource": event["methodArn"]}
            ],
        },
        "context": {"agent": agent, "source": source, "kind": kind},
    }


# --- sequencer ------------------------------------------------------------------


def last_seq() -> int:
    items = ddb.query(
        TableName=TABLE,
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "LOG"}},
        ScanIndexForward=False,
        Limit=1,
        ConsistentRead=True,
    )["Items"]
    return int(items[0]["seq"]["N"]) if items else 0


def existing(msg_id: str) -> dict[str, Any]:
    item = ddb.get_item(
        TableName=TABLE, Key={"pk": {"S": f"MSG#{msg_id}"}, "sk": {"S": "EVENT"}}, ConsistentRead=True
    )["Item"]
    seq = int(item["seq"]["N"])
    log_item = ddb.get_item(
        TableName=TABLE, Key={"pk": {"S": "LOG"}, "sk": {"S": f"E#{seq_key(seq)}"}}, ConsistentRead=True
    )["Item"]
    return json.loads(log_item["event"]["S"])


def append_message(sender: str, body: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    msg_id = body.get("id") or None
    event = MessageEvent.model_validate(
        {
            **({"id": msg_id} if msg_id else {}),
            "from": sender,
            "to": body["to"],
            "kind": body.get("kind", "text"),
            "body": body["body"],
            "thread": msg_id or "pending",
            "ts": now(),
        }
    )
    if not msg_id:
        event = event.model_copy(update={"thread": event.id})
    stats = {"attempts": 0, "log_collisions": 0, "conflicts": 0, "duplicate": False}
    parties = sorted({event.sender, event.to})
    for attempt in range(1, MAX_ATTEMPTS + 1):
        stats["attempts"] = attempt
        seq = last_seq() + 1
        record = json.loads(event.model_dump_json(by_alias=True, exclude_none=True))
        record["seq"] = seq
        line = json.dumps(record, separators=(",", ":"))
        items = [
            {
                "Put": {
                    "TableName": TABLE,
                    "Item": {
                        "pk": {"S": "LOG"},
                        "sk": {"S": f"E#{seq_key(seq)}"},
                        "seq": {"N": str(seq)},
                        "event": {"S": line},
                    },
                    "ConditionExpression": "attribute_not_exists(pk)",
                }
            },
            {
                "Put": {
                    "TableName": TABLE,
                    "Item": {
                        "pk": {"S": f"MSG#{event.id}"},
                        "sk": {"S": "EVENT"},
                        "seq": {"N": str(seq)},
                        "from": {"S": event.sender},
                        "to": {"S": event.to},
                        "kind": {"S": event.kind},
                        "thread": {"S": event.thread},
                    },
                    "ConditionExpression": "attribute_not_exists(pk)",
                }
            },
        ] + [
            {
                "Put": {
                    "TableName": TABLE,
                    "Item": {
                        "pk": {"S": f"AGENT#{party}"},
                        "sk": {"S": f"VIEW#{seq_key(seq)}"},
                        "seq": {"N": str(seq)},
                        "event": {"S": line},
                    },
                }
            }
            for party in parties
        ]
        try:
            ddb.transact_write_items(TransactItems=items)
            return record, stats
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "TransactionCanceledException":
                raise
            reasons = [r.get("Code", "None") for r in exc.response.get("CancellationReasons", [])]
            if len(reasons) > 1 and reasons[1] == "ConditionalCheckFailed":
                stats["duplicate"] = True
                return existing(event.id), stats
            if reasons and reasons[0] == "ConditionalCheckFailed":
                stats["log_collisions"] += 1
            elif "TransactionConflict" in reasons:
                stats["conflicts"] += 1
            else:
                raise
            time.sleep(0.005 * attempt)
    raise RuntimeError(f"sequencer gave up after {MAX_ATTEMPTS} attempts")


# --- push -----------------------------------------------------------------------


def connections(agent: str) -> list[dict[str, Any]]:
    return ddb.query(
        TableName=TABLE,
        KeyConditionExpression="pk = :pk AND begins_with(sk, :c)",
        ExpressionAttributeValues={":pk": {"S": f"AGENT#{agent}"}, ":c": {"S": "CONN#"}},
    )["Items"]


def forget_connection(agent: str, conn: str) -> None:
    ddb.delete_item(TableName=TABLE, Key={"pk": {"S": f"AGENT#{agent}"}, "sk": {"S": f"CONN#{conn}"}})
    ddb.delete_item(TableName=TABLE, Key={"pk": {"S": f"CONN#{conn}"}, "sk": {"S": "AGENT"}})


def post(endpoint: str, conn: str, payload: dict[str, Any]) -> dict[str, Any]:
    started = time.monotonic()
    try:
        mgmt(endpoint).post_to_connection(ConnectionId=conn, Data=json.dumps(payload).encode())
        outcome = "ok"
    except ClientError as exc:
        outcome = exc.response["Error"]["Code"]
    return {"connection": conn, "outcome": outcome, "ms": round((time.monotonic() - started) * 1000)}


def push(record: dict[str, Any]) -> list[dict[str, Any]]:
    results = []
    for party in sorted({record["from"], record["to"]}):
        for row in connections(party):
            conn = row["sk"]["S"].removeprefix("CONN#")
            result = post(row["endpoint"]["S"], conn, {"seq": record["seq"]})
            result["agent"] = party
            if result["outcome"] == "GoneException":
                forget_connection(party, conn)
            results.append(result)
    return results


# --- HTTP routes ----------------------------------------------------------------


def respond(status: int, body: Any, *, text: bool = False) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/x-ndjson" if text else "application/json"},
        "body": body if text else json.dumps(body, default=str),
    }


def http(event: dict[str, Any]) -> dict[str, Any]:
    agent = event["requestContext"]["authorizer"]["lambda"]["agent"]
    method = event["requestContext"]["http"]["method"]
    path = event["requestContext"]["http"]["path"]
    query = event.get("queryStringParameters") or {}

    if method == "POST" and path == "/events":
        body = json.loads(event.get("body") or "{}")
        if body.get("type", "message") != "message":
            return respond(400, {"error": "spike accepts only messages"})
        started = time.monotonic()
        record, stats = append_message(agent, body)
        stats["append_ms"] = round((time.monotonic() - started) * 1000)
        pushes = [] if stats["duplicate"] else push(record)
        return respond(200, {"event": record, "stats": stats, "pushes": pushes})

    if method == "GET" and path == "/events":
        after = int(query.get("after", "0"))
        limit = int(query.get("limit", "100"))
        items = ddb.query(
            TableName=TABLE,
            KeyConditionExpression="pk = :pk AND sk > :after",
            ExpressionAttributeValues={
                ":pk": {"S": f"AGENT#{agent}"},
                ":after": {"S": f"VIEW#{seq_key(after)}"},
            },
            Limit=limit,
        )["Items"]
        lines = "".join(i["event"]["S"] + "\n" for i in items if i["sk"]["S"].startswith("VIEW#"))
        return respond(200, lines, text=True)

    if method == "POST" and path == "/ticket":
        ticket = "tkt_" + secrets.token_urlsafe(24)
        ddb.put_item(
            TableName=TABLE,
            Item={
                "pk": {"S": f"TICKET#{ticket}"},
                "sk": {"S": "AGENT"},
                "agent": {"S": agent},
                "expires": {"N": str(int(time.time()) + TICKET_SECONDS)},
            },
        )
        return respond(200, {"ticket": ticket, "expires_in": TICKET_SECONDS})

    if path.startswith("/debug/conn/"):
        conn = path.removeprefix("/debug/conn/")
        row = ddb.get_item(
            TableName=TABLE, Key={"pk": {"S": f"CONN#{conn}"}, "sk": {"S": "AGENT"}}
        ).get("Item")
        if row is None:
            return respond(404, {"error": "no CONN row", "connection": conn})
        endpoint = row["endpoint"]["S"]
        if method == "GET":
            started = time.monotonic()
            try:
                info = mgmt(endpoint).get_connection(ConnectionId=conn)
                info.pop("ResponseMetadata", None)
                result = {"outcome": "ok", **info}
            except ClientError as exc:
                result = {"outcome": exc.response["Error"]["Code"]}
            result["ms"] = round((time.monotonic() - started) * 1000)
            return respond(200, result)
        if method == "POST":
            return respond(200, post(endpoint, conn, {"probe": time.time()}))

    return respond(404, {"error": f"no route {method} {path}"})


# --- WebSocket routes -----------------------------------------------------------


def websocket(event: dict[str, Any]) -> dict[str, Any]:
    ctx = event["requestContext"]
    conn = ctx["connectionId"]
    route = ctx["routeKey"]
    endpoint = f"https://{ctx['domainName']}/{ctx['stage']}"

    if route == "$connect":
        auth = ctx.get("authorizer") or {}
        agent = auth["agent"]
        expires = str(int(time.time()) + CONN_SECONDS)
        ddb.put_item(
            TableName=TABLE,
            Item={
                "pk": {"S": f"AGENT#{agent}"},
                "sk": {"S": f"CONN#{conn}"},
                "endpoint": {"S": endpoint},
                "expires": {"N": expires},
            },
        )
        ddb.put_item(
            TableName=TABLE,
            Item={
                "pk": {"S": f"CONN#{conn}"},
                "sk": {"S": "AGENT"},
                "agent": {"S": agent},
                "endpoint": {"S": endpoint},
                "expires": {"N": expires},
            },
        )
        log.info("connect %s agent=%s source=%s", conn, agent, auth.get("source"))
        response: dict[str, Any] = {"statusCode": 200}
        if auth.get("source") == "subprotocol":
            response["headers"] = {"Sec-WebSocket-Protocol": "agent-bus"}
        return response

    row = ddb.get_item(
        TableName=TABLE, Key={"pk": {"S": f"CONN#{conn}"}, "sk": {"S": "AGENT"}}
    ).get("Item")
    agent = row["agent"]["S"] if row else "?"

    if route == "$disconnect":
        log.info(
            "disconnect %s agent=%s status=%s reason=%s",
            conn,
            agent,
            ctx.get("disconnectStatusCode"),
            ctx.get("disconnectReason"),
        )
        if row:
            forget_connection(agent, conn)
        return {"statusCode": 200}

    if route == "ping":
        post(endpoint, conn, {"pong": time.time(), "connection": conn, "agent": agent})
        return {"statusCode": 200}

    post(endpoint, conn, {"error": f"unknown route {route}"})
    return {"statusCode": 200}


def handler(event: dict[str, Any], context: Any) -> Any:
    global COLD
    cold, COLD = COLD, False
    if cold:
        log.info("cold start")
    if event.get("type") == "REQUEST" and "methodArn" in event:
        return ws_authorizer(event)
    if event.get("type") == "REQUEST" and "routeArn" in event:
        return http_authorizer(event)
    ctx = event.get("requestContext", {})
    if "eventType" in ctx:
        return websocket(event)
    if "http" in ctx:
        return http(event)
    raise ValueError(f"unrecognised event: {sorted(event)}")
