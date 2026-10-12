"""Bus events: one JSON object per line of log.jsonl.

A message's status is never updated in place. Delivery, acknowledgement and
human activity are separate events, and every derived fact comes from
replaying the log (see state.py). That keeps the log append-only.

`DeliveredEvent.session` is the server process's own id. `HumanTurnEvent`
carries Claude Code's session id from hook input, which the server also
sees as CLAUDE_CODE_SESSION_ID and logs at start.

`seq` is set only by a hub (PLAN.md, v2): its position in the hub's single
order. The local bus leaves it out and orders by position in the file.
"""

from __future__ import annotations

import os
import re
import time
from datetime import UTC, datetime
from typing import Annotated, Literal, Protocol

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


def check_team_name(name: str) -> str:
    """Team names follow the agent-name rule; a team is a hub permission, not an event field."""
    if not AGENT_NAME.fullmatch(name):
        raise ValueError(
            f"invalid team name {name!r}: use 1-32 of a-z, 0-9, '_' or '-', starting alphanumeric"
        )
    return name


class _Event(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")

    v: int = SCHEMA_VERSION
    ts: datetime = Field(default_factory=now)
    seq: int | None = None


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


class SessionReadyEvent(_Event):
    """No longer written; parsed so that logs from 2026-10-01 stay readable.

    The SessionStart hook once wrote it as a readiness signal, but the hook
    fires before the session can take channel events, so it signalled
    nothing (stage 5 rerun). Readiness is now judged by the server itself.
    """

    type: Literal["session_ready"] = "session_ready"
    agent: str
    session: str


Event = Annotated[
    MessageEvent | DeliveredEvent | AckEvent | HumanTurnEvent | SessionReadyEvent,
    Field(discriminator="type"),
]
EVENT_ADAPTER: TypeAdapter[Event] = TypeAdapter(Event)


class MessageHeader(Protocol):
    """What rules about a message need, without its body.

    A `MessageEvent` is one; so is the hub's `MSG#` row, which keeps
    everything but the body so that checking a reply costs one small read.
    """

    @property
    def id(self) -> str: ...
    @property
    def sender(self) -> str: ...
    @property
    def to(self) -> str: ...
    @property
    def kind(self) -> Kind: ...
    @property
    def thread(self) -> str: ...


def participants(event: Event, original: MessageHeader | None = None) -> frozenset[str]:
    """The agents who see `event`: on a hub, the views it is written to.

    A message belongs to its sender and recipient. An ack or a delivery
    belongs to the parties of the message it settles, so the sender learns
    the message was handled; `original` is that message and is required for
    them. Human activity belongs to the agent alone.
    """
    if isinstance(event, MessageEvent):
        return frozenset({event.sender, event.to})
    if isinstance(event, AckEvent | DeliveredEvent):
        if original is None or original.id != event.id:
            raise ValueError(f"{event.type} {event.id} needs the message it settles")
        return frozenset({original.sender, original.to})
    return frozenset({event.agent})


def dump_event(event: Event) -> bytes:
    """One log line, newline included."""
    return event.model_dump_json(by_alias=True, exclude_none=True).encode() + b"\n"
