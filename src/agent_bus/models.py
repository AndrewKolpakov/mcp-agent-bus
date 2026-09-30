"""Bus events: one JSON object per line of log.jsonl.

A message's status is never updated in place. Delivery, acknowledgement and
human activity are separate events, and every derived fact comes from
replaying the log (see state.py). That keeps the log append-only.

Two id spaces coexist and are never joined: `DeliveredEvent.session` is the
server process's own id (the MCP server never sees Claude's session id),
while `HumanTurnEvent.session` is Claude Code's session id from hook input.
"""

from __future__ import annotations

import os
import re
import time
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

SCHEMA_VERSION = 1

Kind = Literal["text", "task", "result", "await_human"]
KINDS: tuple[Kind, ...] = ("text", "task", "result", "await_human")

AGENT_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def new_id() -> str:
    """A ULID: 48-bit millisecond time then 80 random bits, sortable by time."""
    value = (int(time.time() * 1000) << 80) | int.from_bytes(os.urandom(10), "big")
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def now() -> datetime:
    return datetime.now(UTC)


def check_agent_name(name: str) -> str:
    if not AGENT_NAME.fullmatch(name):
        raise ValueError(
            f"invalid agent name {name!r}: use 1-32 of a-z, 0-9, '_' or '-', starting alphanumeric"
        )
    return name


class _Event(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")

    v: int = SCHEMA_VERSION
    ts: datetime = Field(default_factory=now)


class MessageEvent(_Event):
    type: Literal["message"] = "message"
    id: str = Field(default_factory=new_id)
    sender: str = Field(alias="from")
    to: str
    kind: Kind = "text"
    body: str
    reply_to: str | None = None
    thread: str

    @field_validator("sender", "to")
    @classmethod
    def _valid_agent(cls, value: str) -> str:
        return check_agent_name(value)


class DeliveredEvent(_Event):
    type: Literal["delivered"] = "delivered"
    id: str
    to: str
    session: str


class AckEvent(_Event):
    type: Literal["ack"] = "ack"
    id: str
    by: str


class HumanTurnEvent(_Event):
    type: Literal["human_turn"] = "human_turn"
    agent: str
    session: str


Event = Annotated[
    MessageEvent | DeliveredEvent | AckEvent | HumanTurnEvent,
    Field(discriminator="type"),
]
EVENT_ADAPTER: TypeAdapter[Event] = TypeAdapter(Event)


def dump_event(event: Event) -> bytes:
    """One log line, newline included."""
    return event.model_dump_json(by_alias=True, exclude_none=True).encode() + b"\n"
