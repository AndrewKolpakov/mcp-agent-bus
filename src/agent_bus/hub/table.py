"""The hub's one DynamoDB table; rows are laid out in PLAN.md (v2, Хранилище).

Every event gets its `seq` from the LOG partition: read the last seq, then
write `E#<seq+1>` on condition that it does not exist yet, in one
transaction with the event's copy in each participant's view and, for a
message, its `MSG#` row. A writer that loses the race reads again and
retries. `MSG#` carries its own condition, which makes a repeated message
id a duplicate rather than a second event.
"""

from __future__ import annotations

import hashlib
import secrets
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

import boto3
from botocore.exceptions import ClientError

from agent_bus.models import EVENT_ADAPTER, Event, Kind, check_agent_name, check_team_name

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import (
        AttributeValueTypeDef,
        TransactWriteItemTypeDef,
    )

SEQ_WIDTH = 12
LAST_SEQ = 10**SEQ_WIDTH - 1
MAX_ATTEMPTS = 50
CONNECTION_SECONDS = 2 * 3600
"""TTL of connection rows past their last ping; it only collects garbage."""
TOKEN_PREFIX = "ab_"

Item = dict[str, "AttributeValueTypeDef"]


def seq_key(seq: int) -> str:
    return f"{seq:0{SEQ_WIDTH}d}"


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _s(value: str) -> AttributeValueTypeDef:
    return {"S": value}


def _n(value: float) -> AttributeValueTypeDef:
    return {"N": str(value)}


def _str(item: Item, name: str) -> str:
    return item[name]["S"]


def _num(item: Item, name: str) -> float:
    return float(item[name]["N"])


def _code(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "")


@dataclass(frozen=True)
class MessageRef:
    """A message's `MSG#` row: a `MessageHeader` plus where it sits in the log."""

    id: str
    sender: str
    to: str
    kind: Kind
    thread: str
    seq: int


@dataclass(frozen=True)
class Connection:
    id: str
    agent: str
    endpoint: str
    last_ping: float


@dataclass(frozen=True)
class Claim:
    session: str
    connection: str


class SequencerGaveUpError(RuntimeError):
    pass


class DuplicateError(Exception):
    """A message with this id is already in the log."""

    def __init__(self, ref: MessageRef) -> None:
        super().__init__(ref.id)
        self.ref = ref


class Table:
    def __init__(self, name: str, client: DynamoDBClient | None = None) -> None:
        self.name = name
        self.client: DynamoDBClient = client or boto3.client("dynamodb")

    def _get(self, pk: str, sk: str, *, consistent: bool = False) -> Item | None:
        return self.client.get_item(
            TableName=self.name, Key={"pk": _s(pk), "sk": _s(sk)}, ConsistentRead=consistent
        ).get("Item")

    def _query(self, pk: str, prefix: str, *, consistent: bool = False) -> Iterator[Item]:
        kwargs: dict[str, Any] = {
            "TableName": self.name,
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :p)",
            "ExpressionAttributeValues": {":pk": _s(pk), ":p": _s(prefix)},
            "ConsistentRead": consistent,
        }
        while True:
            page = self.client.query(**kwargs)
            yield from page.get("Items", [])
            if "LastEvaluatedKey" not in page:
                return
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    # --- agents, tokens, teams ----------------------------------------------

    def create_agent(self, name: str, now: float | None = None) -> str:
        """Register an agent and return its token, which is stored only as a hash."""
        check_agent_name(name)
        token = TOKEN_PREFIX + secrets.token_urlsafe(32)
        digest = token_hash(token)
        created = time.time() if now is None else now
        self.client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": self.name,
                        "Item": {
                            "pk": _s(f"AGENT#{name}"),
                            "sk": _s("PROFILE"),
                            "token_hash": _s(digest),
                            "created": _n(int(created)),
                        },
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                },
                {
                    "Put": {
                        "TableName": self.name,
                        "Item": {"pk": _s(f"TOKEN#{digest}"), "sk": _s("AGENT"), "agent": _s(name)},
                    }
                },
            ]
        )
        return token

    def agent_for_token(self, token: str) -> str | None:
        item = self._get(f"TOKEN#{token_hash(token)}", "AGENT")
        return _str(item, "agent") if item else None

    def add_member(self, team: str, agent: str) -> None:
        check_team_name(team)
        check_agent_name(agent)
        self.client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": self.name,
                        "Item": {"pk": _s(f"TEAM#{team}"), "sk": _s(f"MEMBER#{agent}")},
                    }
                },
                {
                    "Put": {
                        "TableName": self.name,
                        "Item": {"pk": _s(f"AGENT#{agent}"), "sk": _s(f"TEAM#{team}")},
                    }
                },
            ]
        )

    def remove_member(self, team: str, agent: str) -> None:
        self.client.transact_write_items(
            TransactItems=[
                {
                    "Delete": {
                        "TableName": self.name,
                        "Key": {"pk": _s(f"TEAM#{team}"), "sk": _s(f"MEMBER#{agent}")},
                    }
                },
                {
                    "Delete": {
                        "TableName": self.name,
                        "Key": {"pk": _s(f"AGENT#{agent}"), "sk": _s(f"TEAM#{team}")},
                    }
                },
            ]
        )

    def teams(self, agent: str) -> set[str]:
        return {_str(i, "sk").removeprefix("TEAM#") for i in self._query(f"AGENT#{agent}", "TEAM#")}

    def members(self, team: str) -> set[str]:
        return {
            _str(i, "sk").removeprefix("MEMBER#") for i in self._query(f"TEAM#{team}", "MEMBER#")
        }

    def teammates(self, agent: str) -> set[str]:
        """Everyone sharing at least one team with `agent`, `agent` excluded."""
        found: set[str] = set()
        for team in self.teams(agent):
            found |= self.members(team)
        return found - {agent}

    # --- the log ------------------------------------------------------------

    def last_seq(self) -> int:
        items = self.client.query(
            TableName=self.name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": _s("LOG")},
            ScanIndexForward=False,
            Limit=1,
            ConsistentRead=True,
        ).get("Items", [])
        return int(_num(items[0], "seq")) if items else 0

    def message(self, msg_id: str) -> MessageRef | None:
        item = self._get(f"MSG#{msg_id}", "EVENT", consistent=True)
        if item is None:
            return None
        return MessageRef(
            id=msg_id,
            sender=_str(item, "from"),
            to=_str(item, "to"),
            kind=_str(item, "kind"),  # type: ignore[arg-type]
            thread=_str(item, "thread"),
            seq=int(_num(item, "seq")),
        )

    def event_at(self, seq: int) -> Event:
        item = self._get("LOG", f"E#{seq_key(seq)}", consistent=True)
        if item is None:
            raise LookupError(f"no event at seq {seq}")
        return EVENT_ADAPTER.validate_json(_str(item, "event"))

    def append(self, event: Event, parties: Iterable[str]) -> Event:
        """Give `event` the next seq and write it to the log and each party's view.

        Raises `DuplicateError` when `event` is a message whose id is taken.
        """
        views = sorted(set(parties))
        for attempt in range(1, MAX_ATTEMPTS + 1):
            seq = self.last_seq() + 1
            stored = event.model_copy(update={"seq": seq})
            line = stored.model_dump_json(by_alias=True, exclude_none=True)
            items: list[TransactWriteItemTypeDef] = [
                {
                    "Put": {
                        "TableName": self.name,
                        "Item": {
                            "pk": _s("LOG"),
                            "sk": _s(f"E#{seq_key(seq)}"),
                            "seq": _n(seq),
                            "event": _s(line),
                        },
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                }
            ]
            if stored.type == "message":
                items.append(
                    {
                        "Put": {
                            "TableName": self.name,
                            "Item": {
                                "pk": _s(f"MSG#{stored.id}"),
                                "sk": _s("EVENT"),
                                "seq": _n(seq),
                                "from": _s(stored.sender),
                                "to": _s(stored.to),
                                "kind": _s(stored.kind),
                                "thread": _s(stored.thread),
                            },
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    }
                )
            items.extend(
                {
                    "Put": {
                        "TableName": self.name,
                        "Item": {
                            "pk": _s(f"AGENT#{party}"),
                            "sk": _s(f"VIEW#{seq_key(seq)}"),
                            "seq": _n(seq),
                            "event": _s(line),
                        },
                    }
                }
                for party in views
            )
            try:
                self.client.transact_write_items(TransactItems=items)
                return stored
            except ClientError as exc:
                if _code(exc) != "TransactionCanceledException":
                    raise
                reasons = [
                    r.get("Code", "None") for r in exc.response.get("CancellationReasons", [])
                ]
                if stored.type == "message" and reasons[1:2] == ["ConditionalCheckFailed"]:
                    ref = self.message(stored.id)
                    assert ref is not None
                    raise DuplicateError(ref) from exc
                if (
                    reasons[:1] != ["ConditionalCheckFailed"]
                    and "TransactionConflict" not in reasons
                ):
                    raise
            time.sleep(0.005 * attempt)
        raise SequencerGaveUpError(f"no free seq after {MAX_ATTEMPTS} attempts")

    def view(self, agent: str, after: int, limit: int, max_bytes: int) -> list[str]:
        """Lines of `agent`'s view above `after`, in seq order, capped by count and size."""
        kwargs: dict[str, Any] = {
            "TableName": self.name,
            "KeyConditionExpression": "pk = :pk AND sk BETWEEN :lo AND :hi",
            "ExpressionAttributeValues": {
                ":pk": _s(f"AGENT#{agent}"),
                ":lo": _s(f"VIEW#{seq_key(after + 1)}"),
                ":hi": _s(f"VIEW#{seq_key(LAST_SEQ)}"),
            },
            "Limit": limit,
        }
        lines: list[str] = []
        size = 0
        while len(lines) < limit:
            kwargs["Limit"] = limit - len(lines)
            page = self.client.query(**kwargs)
            for item in page.get("Items", []):
                line = _str(item, "event")
                if lines and size + len(line) + 1 > max_bytes:
                    return lines
                lines.append(line)
                size += len(line) + 1
            if "LastEvaluatedKey" not in page:
                break
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        return lines

    def recent_view(self, agent: str, since: datetime, cap: int) -> list[Event]:
        """`agent`'s events written at or after `since`, oldest first, at most `cap`."""
        kwargs: dict[str, Any] = {
            "TableName": self.name,
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :v)",
            "ExpressionAttributeValues": {":pk": _s(f"AGENT#{agent}"), ":v": _s("VIEW#")},
            "ScanIndexForward": False,
            "Limit": 100,
        }
        found: list[Event] = []
        while len(found) < cap:
            page = self.client.query(**kwargs)
            for item in page.get("Items", []):
                event = EVENT_ADAPTER.validate_json(_str(item, "event"))
                if event.ts < since or len(found) >= cap:
                    return found[::-1]
                found.append(event)
            if "LastEvaluatedKey" not in page:
                break
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        return found[::-1]

    # --- connections and the channel claim ----------------------------------

    def add_connection(self, agent: str, conn: str, endpoint: str, now: float) -> None:
        expires = _n(int(now) + CONNECTION_SECONDS)
        self.client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": self.name,
                        "Item": {
                            "pk": _s(f"AGENT#{agent}"),
                            "sk": _s(f"CONN#{conn}"),
                            "endpoint": _s(endpoint),
                            "last_ping": _n(now),
                            "expires": expires,
                        },
                    }
                },
                {
                    "Put": {
                        "TableName": self.name,
                        "Item": {
                            "pk": _s(f"CONN#{conn}"),
                            "sk": _s("AGENT"),
                            "agent": _s(agent),
                            "expires": expires,
                        },
                    }
                },
            ]
        )

    def connection_agent(self, conn: str) -> str | None:
        item = self._get(f"CONN#{conn}", "AGENT")
        return _str(item, "agent") if item else None

    def connection(self, agent: str, conn: str) -> Connection | None:
        item = self._get(f"AGENT#{agent}", f"CONN#{conn}", consistent=True)
        return self._connection(agent, item) if item else None

    def connections(self, agent: str) -> list[Connection]:
        return [self._connection(agent, i) for i in self._query(f"AGENT#{agent}", "CONN#")]

    @staticmethod
    def _connection(agent: str, item: Item) -> Connection:
        return Connection(
            id=_str(item, "sk").removeprefix("CONN#"),
            agent=agent,
            endpoint=_str(item, "endpoint"),
            last_ping=_num(item, "last_ping"),
        )

    def touch_connection(self, agent: str, conn: str, now: float) -> bool:
        """Record a ping; False if the connection row is gone."""
        try:
            self.client.update_item(
                TableName=self.name,
                Key={"pk": _s(f"AGENT#{agent}"), "sk": _s(f"CONN#{conn}")},
                UpdateExpression="SET last_ping = :now, expires = :exp",
                ConditionExpression="attribute_exists(pk)",
                ExpressionAttributeValues={
                    ":now": _n(now),
                    ":exp": _n(int(now) + CONNECTION_SECONDS),
                },
            )
        except ClientError as exc:
            if _code(exc) == "ConditionalCheckFailedException":
                return False
            raise
        return True

    def forget_connection(self, agent: str, conn: str) -> None:
        for pk, sk in ((f"AGENT#{agent}", f"CONN#{conn}"), (f"CONN#{conn}", "AGENT")):
            self.client.delete_item(TableName=self.name, Key={"pk": _s(pk), "sk": _s(sk)})

    def claim_of(self, agent: str) -> Claim | None:
        item = self._get(f"AGENT#{agent}", "SESSION", consistent=True)
        if item is None:
            return None
        return Claim(session=_str(item, "session"), connection=_str(item, "connection"))

    def put_claim(self, agent: str, claim: Claim, expected: Claim | None, now: float) -> bool:
        """Write the claim only if the row is still `expected` or gone; False if it changed."""
        seen = expected or Claim(session="", connection="")
        try:
            self.client.put_item(
                TableName=self.name,
                Item={
                    "pk": _s(f"AGENT#{agent}"),
                    "sk": _s("SESSION"),
                    "session": _s(claim.session),
                    "connection": _s(claim.connection),
                    "started": _n(int(now)),
                },
                ConditionExpression="attribute_not_exists(pk) OR (#s = :es AND #c = :ec)",
                ExpressionAttributeNames={"#s": "session", "#c": "connection"},
                ExpressionAttributeValues={":es": _s(seen.session), ":ec": _s(seen.connection)},
            )
        except ClientError as exc:
            if _code(exc) == "ConditionalCheckFailedException":
                return False
            raise
        return True

    def delete_claim(self, agent: str, session: str) -> bool:
        try:
            self.client.delete_item(
                TableName=self.name,
                Key={"pk": _s(f"AGENT#{agent}"), "sk": _s("SESSION")},
                ConditionExpression="#s = :s",
                ExpressionAttributeNames={"#s": "session"},
                ExpressionAttributeValues={":s": _s(session)},
            )
        except ClientError as exc:
            if _code(exc) == "ConditionalCheckFailedException":
                return False
            raise
        return True
