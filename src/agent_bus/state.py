"""Facts derived by replaying the event log.

Ordering is by position in the log, never by timestamp: events come from
several processes with independent clocks, and the log order is the only
order all of them agree on.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

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
        return [m for m in self._to(agent) if m.id not in self.delivered]

    def agents(self) -> set[str]:
        names = {m.sender for m in self.messages.values()} | {m.to for m in self.messages.values()}
        return names | set(self.last_human_turn)

    def agent_turns_since_human(self, a: str, b: str) -> int:
        """Messages between `a` and `b`, either way, after either one's latest human turn.

        Counted per pair rather than per thread: otherwise two agents could
        dodge the limit by opening a fresh thread for every message.
        """
        pair = {a, b}
        floor = max(self.last_human_turn.get(a, -1), self.last_human_turn.get(b, -1))
        return sum(
            1
            for m in self.messages.values()
            if {m.sender, m.to} == pair and self.position[m.id] > floor
        )
