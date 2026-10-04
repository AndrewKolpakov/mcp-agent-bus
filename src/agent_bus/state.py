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


@dataclass
class BusState:
    messages: dict[str, MessageEvent] = field(default_factory=dict)
    position: dict[str, int] = field(default_factory=dict)
    delivered: set[str] = field(default_factory=set)
    acked: set[str] = field(default_factory=set)
    last_human_turn: dict[str, int] = field(default_factory=dict)
    """Agent name -> log position of that agent's latest human turn."""

    @classmethod
    def from_events(cls, events: Iterable[Event]) -> BusState:
        state = cls()
        for index, event in enumerate(events):
            state.apply(event, index)
        return state

    def apply(self, event: Event, index: int) -> None:
        if isinstance(event, MessageEvent):
            self.messages.setdefault(event.id, event)
            self.position.setdefault(event.id, index)
        elif isinstance(event, DeliveredEvent):
            self.delivered.add(event.id)
        elif isinstance(event, AckEvent):
            self.acked.add(event.id)
        elif isinstance(event, HumanTurnEvent):
            self.last_human_turn[event.agent] = index

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
