"""What an agent is doing, for the peers list: working, idle or waiting for a human.

Two sources feed it. The session's hooks report its activity: a turn
starts (UserPromptSubmit), ends (Stop, StopFailure), stops at a
permission prompt (PermissionRequest) or goes on past one (PostToolUse).
The bus itself adds the rest: when the agent last sent a message, which
tasks to it are still open (a task counts until it is acked, and replying
acks it), and whether its last word was an await_human that no human turn
has answered yet.

The hooks run in the background, so their reports can land out of order;
each carries the time it was taken on the agent's machine, and a report
older than the one stored is dropped. Comparing those times is sound
because they all come from one machine's clock. Against an await_human
stamped by a hub they are compared across clocks, which NTP keeps within
well under the seconds a turn takes.

A session without hooks, Codex, never reports activity; it reads as
working while a task to it is open and idle otherwise. A closed session
needs no report: its claim on the channel lapses and it reads as offline.

On the local bus all of this is derived from the shared log and the
activity files; on a hub, a peer's view is not ours to read, so the hub
derives it and answers it from `GET /peers`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel

Activity = Literal["working", "idle", "waiting_human"]
ACTIVITIES: tuple[Activity, ...] = ("working", "idle", "waiting_human")
State = Literal["working", "idle", "waiting_human", "offline"]


class ActivityReport(BaseModel):
    """A session's latest report of what it is doing."""

    state: Activity
    at: datetime
    session: str


@dataclass(frozen=True)
class OpenTask:
    msg_id: str
    thread: str
    sender: str
    since: datetime

    def to_json(self) -> dict[str, str]:
        return {
            "msg_id": self.msg_id,
            "thread": self.thread,
            "from": self.sender,
            "since": self.since.isoformat(),
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> OpenTask:
        return cls(
            msg_id=data["msg_id"],
            thread=data["thread"],
            sender=data["from"],
            since=datetime.fromisoformat(data["since"]),
        )


@dataclass(frozen=True)
class BusFacts:
    """What the bus knows of one agent, besides its activity."""

    last_sent: datetime | None = None
    await_human_at: datetime | None = None
    """When it last sent kind=await_human."""
    human_at: datetime | None = None
    """When a human last gave it a turn."""
    tasks: tuple[OpenTask, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Status:
    state: State
    since: datetime | None
    last_sent: datetime | None
    tasks: tuple[OpenTask, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "since": self.since.isoformat() if self.since else None,
            "last_sent": self.last_sent.isoformat() if self.last_sent else None,
            "tasks": [t.to_json() for t in self.tasks],
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Status:
        def stamp(name: str) -> datetime | None:
            value = data.get(name)
            return datetime.fromisoformat(value) if isinstance(value, str) else None

        state = data.get("state")
        return cls(
            state=state if state in (*ACTIVITIES, "offline") else "idle",
            since=stamp("since"),
            last_sent=stamp("last_sent"),
            tasks=tuple(OpenTask.from_json(t) for t in data.get("tasks") or []),
        )


def derive(live: bool, activity: ActivityReport | None, facts: BusFacts) -> Status:
    """The agent's state, from its liveness, its last activity report and the bus.

    Waiting for a human wins: a permission prompt, or an await_human sent
    after the last human turn, unless a turn started after it (another
    agent's message woke the session, and it is working again).
    """
    tasks = tuple(sorted(facts.tasks, key=lambda t: t.since))

    def status(state: State, since: datetime | None) -> Status:
        return Status(state=state, since=since, last_sent=facts.last_sent, tasks=tasks)

    if not live:
        return status("offline", None)
    if activity is not None and activity.state == "waiting_human":
        return status("waiting_human", activity.at)
    awaiting = facts.await_human_at
    if (
        awaiting is not None
        and (facts.human_at is None or facts.human_at < awaiting)
        and not (activity is not None and activity.state == "working" and activity.at > awaiting)
    ):
        return status("waiting_human", awaiting)
    if activity is not None:
        return status(activity.state, activity.at)
    if tasks:
        return status("working", tasks[0].since)
    return status("idle", None)
