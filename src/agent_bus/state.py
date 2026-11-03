"""Facts derived by replaying the event log.

Ordering is by position in the log, never by timestamp: events come from
several processes with independent clocks, and the log order is the only
order all of them agree on.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime

from agent_bus.models import AckEvent, DeliveredEvent, Event, HumanTurnEvent, MessageEvent
from agent_bus.status import BusFacts, OpenTask


@dataclass
class BusState:
    messages: dict[str, MessageEvent] = field(default_factory=dict)
    position: dict[str, int] = field(default_factory=dict)
    delivered: set[str] = field(default_factory=set)
    acked: set[str] = field(default_factory=set)
    last_human_turn: dict[str, int] = field(default_factory=dict)
    """Agent name -> log position of that agent's latest human turn."""
    human_at: dict[str, datetime] = field(default_factory=dict)
    last_sent: dict[str, datetime] = field(default_factory=dict)
    await_human_at: dict[str, datetime] = field(default_factory=dict)

    @classmethod
    def from_events(cls, events: Iterable[Event]) -> BusState:
        state = cls()
        for index, event in enumerate(events):
            state.apply(event, index)
        return state

    def apply(self, event: Event, index: int) -> None:
        if isinstance(event, MessageEvent):
            if event.id in self.messages:
                return
            self.messages[event.id] = event
            self.position[event.id] = index
            self.last_sent[event.sender] = event.ts
            if event.kind == "await_human":
                self.await_human_at[event.sender] = event.ts
        elif isinstance(event, DeliveredEvent):
            self.delivered.add(event.id)
        elif isinstance(event, AckEvent):
            self.acked.add(event.id)
        elif isinstance(event, HumanTurnEvent):
            self.last_human_turn[event.agent] = index
            self.human_at[event.agent] = event.ts

    def _to(self, agent: str) -> list[MessageEvent]:
        found = [m for m in self.messages.values() if m.to == agent]
        return sorted(found, key=lambda m: self.position[m.id])

    def inbox(self, agent: str) -> list[MessageEvent]:
        return self._to(agent)

    def unread(self, agent: str) -> list[MessageEvent]:
        return [m for m in self._to(agent) if m.id not in self.acked]

    def undelivered(self, agent: str) -> list[MessageEvent]:
        """Messages to push: neither delivered nor acked.

        An ack proves the agent got the message by some path (a hook listing,
        the inbox tool), so pushing it again would only repeat it.
        """
        return [m for m in self._to(agent) if m.id not in self.delivered and m.id not in self.acked]

    def history(
        self,
        agent: str,
        *,
        sender: str | None = None,
        thread: str | None = None,
        since: datetime | None = None,
    ) -> list[MessageEvent]:
        """Messages `agent` sent or received, oldest first, narrowed by the filters given.

        `since` compares timestamps, which is good enough for a person
        asking "since this morning"; order stays the log's.
        """
        found = [
            m
            for m in self.messages.values()
            if agent in (m.sender, m.to)
            and (sender is None or m.sender == sender)
            and (thread is None or m.thread == thread)
            and (since is None or m.ts >= since)
        ]
        return sorted(found, key=lambda m: self.position[m.id])

    def facts(self, agent: str) -> BusFacts:
        """What this log says of `agent`: on a hub, only as much as one's own view holds."""
        tasks = tuple(
            OpenTask(msg_id=m.id, thread=m.thread, sender=m.sender, since=m.ts)
            for m in self.unread(agent)
            if m.kind == "task"
        )
        return BusFacts(
            last_sent=self.last_sent.get(agent),
            await_human_at=self.await_human_at.get(agent),
            human_at=self.human_at.get(agent),
            tasks=tasks,
        )

    def agents(self) -> set[str]:
        names = {m.sender for m in self.messages.values()} | {m.to for m in self.messages.values()}
        return names | set(self.last_human_turn)

    def messages_between(self, a: str, b: str, *, since: datetime) -> int:
        """Messages between `a` and `b`, either way, written at or after `since`.

        Counted per pair rather than per thread, so opening a fresh thread for
        every message does not dodge the rate limit. Timestamps are compared
        here, unlike ordering: every writer runs on this one machine, and a
        window of minutes tolerates any clock skew between its processes.
        """
        pair = {a, b}
        return sum(1 for m in self.messages.values() if {m.sender, m.to} == pair and m.ts >= since)
