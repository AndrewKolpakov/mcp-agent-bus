"""Bus operations for one agent, shared by the CLI, the MCP server and hooks."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from agent_bus import leaks
from agent_bus.config import DEFAULT_RATE_LIMIT, DEFAULT_RATE_WINDOW_SECONDS
from agent_bus.models import (
    MAX_ATTACHMENT_BYTES,
    MAX_ATTACHMENTS,
    AckEvent,
    Attachment,
    DeliveredEvent,
    HumanTurnEvent,
    Kind,
    MessageEvent,
    MessageHeader,
    check_agent_name,
    new_id,
    now,
)
from agent_bus.registry import Registry, SessionRegistry
from agent_bus.state import BusState
from agent_bus.status import Activity, ActivityReport, Status, derive
from agent_bus.store import EventStore, Store, sha256_of

MAX_BODY_BYTES = 64 * 1024
DEFAULT_HISTORY = 20
MAX_HISTORY = 200


class BusError(ValueError):
    """A request the bus refuses; the message is safe to show to the model."""


_RELATIVE = re.compile(r"(\d+)\s*([smhdw])")
_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


def parse_since(text: str) -> datetime:
    """A point in time: ISO-8601 (no offset means UTC), or an age like '30m', '2h', '1d'."""
    text = text.strip()
    relative = _RELATIVE.fullmatch(text)
    if relative:
        amount, unit = relative.groups()
        return now() - timedelta(**{_UNITS[unit]: int(amount)})
    try:
        stamp = datetime.fromisoformat(text)
    except ValueError:
        raise BusError(
            f"since={text!r} is neither ISO-8601 (2026-11-03T09:00:00Z) nor an age like 2h"
        ) from None
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=UTC)


class RateLimitError(BusError):
    pass


class HubUnavailableError(BusError):
    """The hub could not be reached or failed; the request may be retried."""


# Ping-pong is agents answering answers ("done" -> "thanks" -> "you're welcome").
# A result or an await_human ends its chain: it can be acked, and a result can
# be followed by a new task, but never answered with more talk. The rate limit
# is the backstop for loops this rule does not foresee; real work peaked at 10
# messages per 5 minutes in the stage-5 history, so 30 leaves 3x headroom.
ALLOWED_REPLIES: dict[Kind, frozenset[Kind]] = {
    "result": frozenset({"task"}),
    "await_human": frozenset(),
}


def check_reply(original: MessageHeader, kind: Kind) -> None:
    """Refuse answering a message whose kind ends its chain with anything but what it allows."""
    allowed = ALLOWED_REPLIES.get(original.kind)
    if allowed is not None and kind not in allowed:
        hint = "; start the next step with kind='task'" if allowed else ""
        raise BusError(
            f"message {original.id} is kind={original.kind!r}, which ends its chain: "
            f"do not answer it, ack it instead{hint}"
        )


def check_rate(recent: int, me: str, to: str, *, limit: int, window: float) -> None:
    """Refuse a message once `recent` messages between the pair fill the window."""
    if recent >= limit:
        raise RateLimitError(
            f"{recent} messages between {me} and {to} in the last "
            f"{window:g}s (limit {limit}); this looks like a loop. "
            "Send one message with kind='await_human' and wait for a human"
        )


def check_secrets(text: str, where: str = "body") -> None:
    """Refuse text that looks like it carries a secret; the hub runs the same check."""
    findings = leaks.scan(text, where)
    if findings:
        raise BusError(leaks.refusal(findings))


def read_attachments(paths: Sequence[str | Path]) -> list[tuple[Attachment, bytes]]:
    """Read, hash and check the files to attach, before anything is sent.

    A file is checked for secrets like a body; bytes that are not UTF-8
    are read as text where they can be, since a key in a binary still leaks.
    """
    if len(paths) > MAX_ATTACHMENTS:
        raise BusError(f"at most {MAX_ATTACHMENTS} attachments per message")
    files: list[tuple[Attachment, bytes]] = []
    for raw in paths:
        path = Path(raw).expanduser()
        try:
            size = path.stat().st_size
            if not path.is_file():
                raise BusError(f"attachment {raw} is not a regular file")
            if size > MAX_ATTACHMENT_BYTES:
                raise BusError(f"attachment {raw} is {size} bytes, over {MAX_ATTACHMENT_BYTES}")
            data = path.read_bytes()
        except OSError as exc:
            raise BusError(f"cannot read attachment {raw}: {exc.strerror or exc}") from exc
        check_secrets(data.decode("utf-8", errors="replace"), f"attachment {path.name}")
        try:
            attachment = Attachment(sha256=sha256_of(data), name=path.name, size=len(data))
        except ValueError as exc:
            raise BusError(f"attachment {raw}: {exc}") from exc
        files.append((attachment, data))
    return files


@dataclass(frozen=True)
class Peer:
    name: str
    live: bool
    unread_by_them: int
    status: Status

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "live": self.live,
            "unread_by_them": self.unread_by_them,
            **self.status.to_json(),
        }


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
        self,
        to: str,
        body: str,
        kind: Kind = "text",
        reply_to: str | None = None,
        attachments: Sequence[str | Path] = (),
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
        check_secrets(body)
        files = read_attachments(attachments)

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
            check_rate(recent, self.me, to, limit=self.rate_limit, window=self.rate_window)

        for attachment, data in files:
            self.store.put_attachment(attachment, data)
        message = self.store.append(
            MessageEvent(
                id=msg_id,
                sender=self.me,
                to=to,
                kind=kind,
                body=body,
                reply_to=reply_to,
                thread=thread,
                attachments=tuple(a for a, _ in files) or None,
            )
        )
        # Replying settles the message being answered, but only when it was ours.
        if original is not None and original.to == self.me and original.id not in state.acked:
            self.store.append(AckEvent(id=original.id, by=self.me))
        return message

    def inbox(self, *, unread_only: bool = True) -> list[MessageEvent]:
        state = self.state()
        return state.unread(self.me) if unread_only else state.inbox(self.me)

    def history(
        self,
        *,
        sender: str | None = None,
        thread: str | None = None,
        since: datetime | None = None,
        last: int | None = DEFAULT_HISTORY,
    ) -> list[MessageEvent]:
        """Messages sent and received, oldest first: the newest `last` that pass the filters.

        `last=None` lifts the cap; the CLI may, the MCP tool never does,
        since one unbounded answer once came to 6.4 MB.
        """
        if last is not None and last < 1:
            raise BusError("last must be at least 1")
        if sender is not None:
            try:
                check_agent_name(sender)
            except ValueError as exc:
                raise BusError(str(exc)) from exc
        found = self.state().history(self.me, sender=sender, thread=thread, since=since)
        return found if last is None else found[-last:]

    def attachments(self, msg_id: str) -> list[tuple[Attachment, Path]]:
        """The message's attachments as local files, each checked against its sha256."""
        message = self.state().messages.get(msg_id)
        if message is None or self.me not in (message.sender, message.to):
            raise BusError(f"unknown message id: {msg_id}")
        return [(a, self.store.attachment_path(message, a)) for a in message.attachments or ()]

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

    def report_activity(self, session: str, state: Activity, at: datetime | None = None) -> None:
        self.registry.report_activity(
            self.me, ActivityReport(state=state, at=at or now(), session=session)
        )

    def peers(self) -> list[Peer]:
        """Agents one may write to, with what they are doing and what they left unacked.

        On a hub the state is one's own view, so `unread_by_them` counts only
        one's own messages; locally it counts everyone's. The status comes
        from the hub there, and is derived here from the log and the
        activity files on the local bus (status.py).
        """
        state = self.state()
        live = self.registry.live_agents()
        team = self.registry.teammates(self.me)
        names = (state.agents() | live if team is None else team) - {self.me}
        derived = self.registry.statuses(self.me)
        peers = []
        for name in sorted(names):
            status = derived.get(name) if derived is not None else None
            if status is None:
                activity = (
                    self.registry.activity(name) if isinstance(self.registry, Registry) else None
                )
                status = derive(name in live, activity, state.facts(name))
            peers.append(
                Peer(
                    name=name,
                    live=name in live,
                    unread_by_them=len(state.unread(name)),
                    status=status,
                )
            )
        return peers
