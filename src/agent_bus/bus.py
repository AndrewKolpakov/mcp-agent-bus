"""Bus operations for one agent, shared by the CLI, the MCP server and hooks."""

from __future__ import annotations

from dataclasses import dataclass

from agent_bus.models import (
    AckEvent,
    DeliveredEvent,
    HumanTurnEvent,
    Kind,
    MessageEvent,
    SessionReadyEvent,
    check_agent_name,
    new_id,
)
from agent_bus.registry import Registry
from agent_bus.state import BusState
from agent_bus.store import Store

MAX_BODY_BYTES = 64 * 1024


class BusError(ValueError):
    """A request the bus refuses; the message is safe to show to the model."""


class TurnLimitError(BusError):
    pass


@dataclass(frozen=True)
class Peer:
    name: str
    live: bool
    unread_by_them: int


class Bus:
    def __init__(self, store: Store, me: str, max_agent_turns: int) -> None:
        self.store = store
        self.me = check_agent_name(me)
        self.max_agent_turns = max_agent_turns

    def state(self) -> BusState:
        return BusState.from_events(self.store.read_all().events)

    def send(
        self, to: str, body: str, kind: Kind = "text", reply_to: str | None = None
    ) -> MessageEvent:
        try:
            to = check_agent_name(to)
        except ValueError as exc:
            raise BusError(str(exc)) from exc
        if to == self.me:
            raise BusError("cannot send a message to yourself")
        if not body.strip():
            raise BusError("message body is empty")
        if len(body.encode()) > MAX_BODY_BYTES:
            raise BusError(f"message body exceeds {MAX_BODY_BYTES} bytes")

        state = self.state()
        msg_id = new_id()
        original: MessageEvent | None = None
        if reply_to is not None:
            original = state.messages.get(reply_to)
            if original is None:
                raise BusError(f"unknown message id for reply_to: {reply_to}")
            thread = original.thread
        else:
            thread = msg_id

        # await_human is how an agent at the limit hands control back to the
        # humans, so it is never refused.
        if kind != "await_human":
            turns = state.agent_turns_since_human(self.me, to)
            if turns >= self.max_agent_turns:
                raise TurnLimitError(
                    f"{turns} messages between {self.me} and {to} since a human last spoke "
                    f"(limit {self.max_agent_turns}); send kind='await_human' and wait for a human"
                )

        message = MessageEvent(
            id=msg_id, sender=self.me, to=to, kind=kind, body=body, reply_to=reply_to, thread=thread
        )
        self.store.append(message)
        # Replying settles the message being answered, but only when it was ours.
        if original is not None and original.to == self.me and original.id not in state.acked:
            self.store.append(AckEvent(id=original.id, by=self.me))
        return message

    def inbox(self, *, unread_only: bool = True) -> list[MessageEvent]:
        state = self.state()
        return state.unread(self.me) if unread_only else state.inbox(self.me)

    def ack(self, msg_id: str) -> bool:
        """Mark a message to us handled; False if it already was."""
        state = self.state()
        message = state.messages.get(msg_id)
        if message is None:
            raise BusError(f"unknown message id: {msg_id}")
        if message.to != self.me:
            raise BusError(f"message {msg_id} is addressed to {message.to}, not {self.me}")
        if msg_id in state.acked:
            return False
        self.store.append(AckEvent(id=msg_id, by=self.me))
        return True

    def mark_delivered(self, msg_id: str, session: str) -> None:
        self.store.append(DeliveredEvent(id=msg_id, to=self.me, session=session))

    def record_human_turn(self, session: str) -> None:
        self.store.append(HumanTurnEvent(agent=self.me, session=session))

    def record_session_ready(self, session: str) -> None:
        self.store.append(SessionReadyEvent(agent=self.me, session=session))

    def peers(self) -> list[Peer]:
        state = self.state()
        live = Registry(self.store).live()
        names = (state.agents() | set(live)) - {self.me}
        return [
            Peer(name=name, live=name in live, unread_by_them=len(state.unread(name)))
            for name in sorted(names)
        ]
