from __future__ import annotations

from datetime import timedelta

from agent_bus.bus import Bus
from agent_bus.models import now
from agent_bus.registry import Registry
from agent_bus.status import ActivityReport, BusFacts, OpenTask, derive
from agent_bus.store import Store

T0 = now()


def at(minutes: float) -> ActivityReport:
    return ActivityReport(state="working", at=T0 + timedelta(minutes=minutes), session="s")


def test_offline_wins_over_everything() -> None:
    assert derive(False, at(0), BusFacts()).state == "offline"


def test_activity_report_gives_state_and_since() -> None:
    status = derive(True, at(3), BusFacts(last_sent=T0))
    assert (status.state, status.since, status.last_sent) == (
        "working",
        T0 + timedelta(minutes=3),
        T0,
    )


def test_await_human_waits_until_a_human_turn_or_new_work() -> None:
    sent = BusFacts(await_human_at=T0)
    idle = ActivityReport(state="idle", at=T0 + timedelta(seconds=5), session="s")
    assert derive(True, idle, sent).state == "waiting_human"
    answered = BusFacts(await_human_at=T0, human_at=T0 + timedelta(minutes=1))
    assert derive(True, idle, answered).state == "idle"
    assert derive(True, at(2), sent).state == "working"


def test_a_permission_prompt_is_waiting_for_a_human() -> None:
    prompt = ActivityReport(state="waiting_human", at=T0, session="s")
    assert derive(True, prompt, BusFacts()).state == "waiting_human"


def test_without_hooks_an_open_task_reads_as_working() -> None:
    task = OpenTask(msg_id="M", thread="T", sender="alice", since=T0)
    status = derive(True, None, BusFacts(tasks=(task,)))
    assert (status.state, status.since, status.tasks) == ("working", T0, (task,))
    assert derive(True, None, BusFacts()).state == "idle"


def test_local_peers_show_activity_last_sent_and_open_tasks(
    store: Store, alice: Bus, bob: Bus
) -> None:
    registry = Registry(store)
    registry.claim("bob", "s-bob", 1)  # pid 1 is always alive
    task = alice.send("bob", "review this", kind="task")
    bob.send("alice", "a question first")
    bob.report_activity("s-bob", "working")

    peer = {p.name: p for p in alice.peers()}["bob"]
    assert peer.live
    assert peer.status.state == "working"
    assert peer.status.last_sent is not None
    assert [(t.msg_id, t.sender) for t in peer.status.tasks] == [(task.id, "alice")]
    assert peer.to_json()["tasks"][0]["thread"] == task.thread

    bob.send("alice", "done", kind="result", reply_to=task.id)
    assert {p.name: p for p in alice.peers()}["bob"].status.tasks == ()


def test_an_older_activity_report_does_not_replace_a_newer_one(store: Store) -> None:
    registry = Registry(store)
    registry.report_activity("bob", ActivityReport(state="idle", at=T0, session="s"))
    registry.report_activity(
        "bob", ActivityReport(state="working", at=T0 - timedelta(seconds=1), session="s")
    )
    report = registry.activity("bob")
    assert report is not None
    assert report.state == "idle"
