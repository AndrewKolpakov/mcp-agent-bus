"""Bus operations for one agent, shared by the CLI, the MCP server and hooks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from agent_bus.config import DEFAULT_RATE_LIMIT, DEFAULT_RATE_WINDOW_SECONDS
from agent_bus.models import (
    AckEvent,
    DeliveredEvent,
    HumanTurnEvent,
    Kind,
    MessageEvent,
    check_agent_name,
    new_id,
    now,
)
from agent_bus.registry import Registry, SessionRegistry
from agent_bus.state import BusState
from agent_bus.store import EventStore, Store

MAX_BODY_BYTES = 64 * 1024


class BusError(ValueError):
    """A request the bus refuses; the message is safe to show to the model."""


class RateLimitError(BusError):
    pass


# Ping-pong is agents answering answers ("done" -> "thanks" -> "you're welcome").
# A result or an await_human ends its chain: it can be acked, and a result can
# be followed by a new task, but never answered with more talk. The rate limit
# is the backstop for loops this rule does not foresee; real work peaked at 10
# messages per 5 minutes in the stage-5 history, so 30 leaves 3x headroom.
ALLOWED_REPLIES: dict[Kind, frozenset[Kind]] = {
    "result": frozenset({"task"}),
    "await_human": frozenset(),
}


def check_reply(original: MessageEvent, kind: Kind) -> None:
    """Refuse answering a message whose kind ends its chain with anything but what it allows."""
    allowed = ALLOWED_REPLIES.get(original.kind)
    if allowed is not None and kind not in allowed:
        hint = "; start the next step with kind='task'" if allowed else ""
        raise BusError(
            f"message {original.id} is kind={original.kind!r}, which ends its chain: "
            f"do not answer it, ack it instead{hint}"
        )


@dataclass(frozen=True)
class Peer:
    name: str
    live: bool
    unread_by_them: int


class Bus:
    def __init__(
        self,
        store: EventStore,
        me: str,
        *,
        registry: SessionRegistry | None = None,
        rate_limit: int = DEFAULT_RATE_LIMIT,
        rate_window: float = DEFAULT_RATE_WINDOW_SECONDS,
    ) -> None:
        self.store = store
        if registry is None:
            if not isinstance(store, Store):
                raise TypeError("a store other than the local one needs its own registry")
            registry = Registry(store)
        self.registry = registry
        self.me = check_agent_name(me)
        self.rate_limit = rate_limit
        self.rate_window = rate_window

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

        if original is not None:
            check_reply(original, kind)

        # await_human is how agents hand control back to the humans, so the
        # rate limit never refuses it.
        if kind != "await_human":
            recent = state.messages_between(
                self.me, to, since=now() - timedelta(seconds=self.rate_window)
            )
            if recent >= self.rate_limit:
                raise RateLimitError(
                    f"{recent} messages between {self.me} and {to} in the last "
                    f"{self.rate_window:g}s (limit {self.rate_limit}); this looks like a loop. "
                    "Send one message with kind='await_human' and wait for a human"
                )

        message = self.store.append(
            MessageEvent(
                id=msg_id,
                sender=self.me,
                to=to,
                kind=kind,
                body=body,
                reply_to=reply_to,
                thread=thread,
            )
        )
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

    def peers(self) -> list[Peer]:
        """Agents one may write to, each with how many of one's messages they left unacked.

        On a hub the state is one's own view, so `unread_by_them` counts only
        one's own messages; locally it counts everyone's.
        """
        state = self.state()
        live = self.registry.live_agents()
        team = self.registry.teammates(self.me)
        names = (state.agents() | live if team is None else team) - {self.me}
        return [
            Peer(name=name, live=name in live, unread_by_them=len(state.unread(name)))
            for name in sorted(names)
        ]
