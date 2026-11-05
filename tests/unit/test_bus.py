from __future__ import annotations

from datetime import timedelta
from typing import cast

import pytest

from agent_bus.bus import MAX_BODY_BYTES, Bus, BusError, RateLimitError, check_reply
from agent_bus.models import AckEvent, Event, MessageEvent, now
from agent_bus.registry import Registry
from agent_bus.status import ActivityReport, Status
from agent_bus.store import Store


def test_new_message_starts_its_own_thread(alice: Bus, bob: Bus) -> None:
    msg = alice.send("bob", "hello", kind="task")
    assert msg.thread == msg.id
    assert [m.id for m in bob.inbox()] == [msg.id]
    assert alice.inbox() == []


def test_reply_inherits_thread_and_acks_the_original(alice: Bus, bob: Bus) -> None:
    question = alice.send("bob", "question")
    answer = bob.send("alice", "answer", kind="result", reply_to=question.id)
    assert answer.thread == question.thread
    assert answer.reply_to == question.id
    assert bob.inbox() == []
    assert [m.id for m in alice.inbox()] == [answer.id]


def test_reply_to_a_message_not_addressed_to_me_does_not_ack_it(alice: Bus, bob: Bus) -> None:
    mine = alice.send("bob", "for bob")
    alice.send("bob", "follow-up", reply_to=mine.id)
    acks = [e for e in bob.store.read_all().events if isinstance(e, AckEvent)]
    assert acks == []
    assert len(bob.inbox()) == 2


def test_rate_limit_counts_the_pair_across_threads(alice: Bus, bob: Bus) -> None:
    for i in range(2):
        alice.send("bob", f"a{i}")
        bob.send("alice", f"b{i}")
    with pytest.raises(RateLimitError, match="await_human"):
        alice.send("bob", "a fresh thread does not dodge the limit")


def test_await_human_passes_the_rate_limit(alice: Bus, bob: Bus) -> None:
    for i in range(2):
        alice.send("bob", f"a{i}")
        bob.send("alice", f"b{i}")
    assert alice.send("bob", "waiting for a human", kind="await_human").kind == "await_human"
    with pytest.raises(RateLimitError):
        bob.send("alice", "still over the limit")


def test_messages_outside_the_window_do_not_count(store: Store) -> None:
    alice = Bus(store, "alice", rate_limit=2, rate_window=60)
    old = now() - timedelta(minutes=5)
    for i in range(5):
        store.append(MessageEvent(sender="alice", to="bob", body=f"old{i}", thread="T", ts=old))
    alice.send("bob", "one")
    alice.send("bob", "two")
    with pytest.raises(RateLimitError):
        alice.send("bob", "three within a minute")


def test_a_long_task_chain_is_never_refused(store: Store) -> None:
    # Task, result, next task, ... far past any message count.
    alice = Bus(store, "alice", rate_limit=1000)
    bob = Bus(store, "bob", rate_limit=1000)
    task = alice.send("bob", "step 0", kind="task")
    for i in range(1, 100):
        result = bob.send("alice", f"done {i - 1}", kind="result", reply_to=task.id)
        task = alice.send("bob", f"step {i}", kind="task", reply_to=result.id)
    assert task.body == "step 99"


def test_a_result_ends_its_chain(alice: Bus, bob: Bus) -> None:
    task = alice.send("bob", "do it", kind="task")
    result = bob.send("alice", "done", kind="result", reply_to=task.id)
    for kind in ("text", "result", "await_human"):
        with pytest.raises(BusError, match=r"ends its chain.*kind='task'"):
            alice.send("bob", "thanks!", kind=kind, reply_to=result.id)
    assert alice.send("bob", "next step", kind="task", reply_to=result.id).kind == "task"


def test_await_human_cannot_be_answered(alice: Bus, bob: Bus) -> None:
    stop = alice.send("bob", "over to the humans", kind="await_human")
    for kind in ("text", "task", "result", "await_human"):
        with pytest.raises(BusError, match="ends its chain"):
            bob.send("alice", "ok", kind=kind, reply_to=stop.id)
    assert bob.ack(stop.id) is True


def test_text_replies_stay_allowed(alice: Bus, bob: Bus) -> None:
    question = alice.send("bob", "which option?")
    answer = bob.send("alice", "option 2", reply_to=question.id)
    assert alice.send("bob", "agreed", reply_to=answer.id).kind == "text"


def test_a_thread_post_lands_in_the_thread_without_replying_or_acking(
    alice: Bus, bob: Bus
) -> None:
    task = alice.send("bob", "do it", kind="task")
    result = bob.send("alice", "done", kind="result", reply_to=task.id)
    acks_before = [e for e in alice.store.read_all().events if isinstance(e, AckEvent)]
    follow_up = alice.send("bob", "one more thing", kind="task", thread=task.thread)
    assert follow_up.thread == task.thread
    assert follow_up.reply_to is None
    assert follow_up.id != task.thread
    acks_after = [e for e in alice.store.read_all().events if isinstance(e, AckEvent)]
    assert acks_after == acks_before
    # The result stays unacked: a thread post settles nothing.
    assert result.id not in alice.state().acked
    assert [m.id for m in alice.history(thread=task.thread)] == [task.id, result.id, follow_up.id]
    assert [m.id for m in bob.inbox()] == [follow_up.id]


def test_a_thread_post_works_in_both_directions_for_task_and_text(store: Store) -> None:
    alice = Bus(store, "alice", rate_limit=1000)
    bob = Bus(store, "bob", rate_limit=1000)
    root = alice.send("bob", "start", kind="task")
    for sender, to in ((alice, "bob"), (bob, "alice")):
        for kind in ("task", "text"):
            posted = sender.send(to, f"{kind} from {sender.me}", kind=kind, thread=root.thread)  # type: ignore[arg-type]
            assert (posted.thread, posted.reply_to, posted.kind) == (root.thread, None, kind)


@pytest.mark.parametrize("kind", ["result", "await_human"])
def test_a_thread_post_of_a_result_or_a_handoff_is_refused(
    alice: Bus, bob: Bus, kind: str
) -> None:
    task = alice.send("bob", "do it", kind="task")
    count = len(alice.store.read_all().events)
    with pytest.raises(
        BusError,
        match="a thread post must be kind task or text; a result answers with reply_to",
    ):
        bob.send("alice", "x", kind=kind, thread=task.thread)  # type: ignore[arg-type]
    assert len(alice.store.read_all().events) == count


def test_a_thread_post_does_not_ack_a_message_addressed_to_the_sender(
    alice: Bus, bob: Bus
) -> None:
    task = alice.send("bob", "do it", kind="task")
    bob.send("alice", "also this", kind="text", thread=task.thread)
    # bob posted into the thread of a task addressed to him: it stays unacked.
    assert task.id not in bob.state().acked
    assert [m.id for m in bob.inbox()] == [task.id]
    assert [e for e in bob.store.read_all().events if isinstance(e, AckEvent)] == []


def test_a_text_follow_up_after_a_result_works_by_thread(alice: Bus, bob: Bus) -> None:
    task = alice.send("bob", "do it", kind="task")
    first = bob.send("alice", "done", kind="result", reply_to=task.id)
    second = bob.send("alice", "also this", kind="text", thread=task.thread)
    assert second.kind == "text"
    assert second.thread == first.thread == task.thread
    assert second.reply_to is None


def test_a_thread_post_into_a_result_rooted_thread_is_allowed(alice: Bus, bob: Bus) -> None:
    result = alice.send("bob", "fyi, done", kind="result")
    posted = bob.send("alice", "next", kind="task", thread=result.thread)
    assert posted.thread == result.id


def test_thread_and_reply_to_together_are_refused(alice: Bus, bob: Bus) -> None:
    task = alice.send("bob", "do it", kind="task")
    count = len(alice.store.read_all().events)
    with pytest.raises(BusError, match="pass reply_to or thread, not both"):
        alice.send("bob", "x", kind="task", reply_to=task.id, thread=task.thread)
    assert len(alice.store.read_all().events) == count


def test_a_thread_post_into_an_unknown_thread_is_refused(alice: Bus) -> None:
    with pytest.raises(BusError, match="unknown thread: NOPE"):
        alice.send("bob", "x", thread="NOPE")


def test_a_thread_post_must_name_the_threads_root_not_a_later_message(
    alice: Bus, bob: Bus
) -> None:
    task = alice.send("bob", "do it", kind="task")
    result = bob.send("alice", "done", kind="result", reply_to=task.id)
    assert result.thread == task.id
    with pytest.raises(BusError, match=f"unknown thread: {result.id}"):
        alice.send("bob", "x", thread=result.id)


def test_a_third_agent_cannot_post_into_a_thread(store: Store, alice: Bus, bob: Bus) -> None:
    carol = Bus(store, "carol", rate_limit=4)
    task = alice.send("bob", "do it", kind="task")
    count = len(store.read_all().events)
    for to in ("alice", "bob"):
        with pytest.raises(BusError, match=f"not a participant of thread {task.thread}"):
            carol.send(to, "let me in", thread=task.thread)
    assert len(store.read_all().events) == count


def test_a_thread_posts_recipient_must_be_the_other_participant(
    store: Store, alice: Bus, bob: Bus
) -> None:
    task = alice.send("bob", "do it", kind="task")
    with pytest.raises(BusError, match="must be the thread's other participant"):
        alice.send("carol", "x", thread=task.thread)
    with pytest.raises(BusError, match="must be the thread's other participant"):
        bob.send("carol", "x", thread=task.thread)
    with pytest.raises(BusError, match="yourself"):
        alice.send("alice", "x", thread=task.thread)


def test_a_thread_post_keeps_the_rate_limit_secret_and_body_checks(alice: Bus, bob: Bus) -> None:
    task = alice.send("bob", "do it", kind="task")
    with pytest.raises(BusError, match="empty"):
        alice.send("bob", "  ", thread=task.thread)
    with pytest.raises(BusError, match="exceeds"):
        alice.send("bob", "x" * (MAX_BODY_BYTES + 1), thread=task.thread)
    with pytest.raises(BusError):
        alice.send("bob", "key AKIAIOSFODNN7EXAMPLE", thread=task.thread)
    for i in range(3):
        alice.send("bob", f"a{i}", thread=task.thread)
    with pytest.raises(RateLimitError, match="await_human"):
        alice.send("bob", "over the limit", thread=task.thread)
    # The hand-off is not a thread post; it is the way past the limit.
    handoff = alice.send("bob", "handoff", kind="await_human")
    assert handoff.kind == "await_human"


def test_replying_to_a_result_is_still_refused_by_the_loop_guard(alice: Bus, bob: Bus) -> None:
    task = alice.send("bob", "do it", kind="task")
    result = bob.send("alice", "done", kind="result", reply_to=task.id)
    with pytest.raises(BusError, match="ends its chain"):
        alice.send("bob", "thanks", kind="text", reply_to=result.id)
    # A thread post is the way on, and the loop guard is untouched afterwards.
    alice.send("bob", "next", kind="task", thread=task.thread)
    with pytest.raises(BusError, match="ends its chain"):
        alice.send("bob", "thanks again", kind="text", reply_to=result.id)


def test_reply_with_no_thread_argument_is_unchanged(alice: Bus, bob: Bus) -> None:
    task = alice.send("bob", "do it", kind="task")
    result = bob.send("alice", "done", kind="result", reply_to=task.id)
    assert (result.thread, result.reply_to) == (task.thread, task.id)
    assert task.id in bob.state().acked


def test_ack_is_idempotent_and_refuses_foreign_or_unknown(alice: Bus, bob: Bus) -> None:
    msg = alice.send("bob", "x")
    assert bob.ack(msg.id) is True
    assert bob.ack(msg.id) is False
    with pytest.raises(BusError, match="addressed to bob"):
        alice.ack(msg.id)
    with pytest.raises(BusError, match="unknown message id"):
        bob.ack("NOPE")


@pytest.mark.parametrize(
    ("to", "body", "error"),
    [
        ("alice", "x", "yourself"),
        ("Bob!", "x", "invalid agent name"),
        ("bob", "   ", "empty"),
        ("bob", "x" * (MAX_BODY_BYTES + 1), "exceeds"),
    ],
)
def test_send_refuses_bad_requests(alice: Bus, to: str, body: str, error: str) -> None:
    with pytest.raises(BusError, match=error):
        alice.send(to, body)


def test_reply_to_unknown_id_is_refused(alice: Bus) -> None:
    with pytest.raises(BusError, match="unknown message id"):
        alice.send("bob", "x", reply_to="NOPE")


def test_peers_lists_other_agents_with_their_unread(alice: Bus, bob: Bus) -> None:
    alice.send("bob", "one")
    alice.send("carol", "two")
    peers = {p.name: p for p in alice.peers()}
    assert set(peers) == {"bob", "carol"}
    assert peers["bob"].unread_by_them == 1
    assert not peers["bob"].live


class TeamRegistry:
    """A registry as a hub presents it: a fixed team, some of it live."""

    def __init__(self, team: set[str], live: set[str]) -> None:
        self.team = team
        self.live = live

    def claim(self, agent: str, session: str, pid: int) -> bool:
        return True

    def heartbeat(self, agent: str, session: str) -> bool:
        return True

    def release(self, agent: str, session: str) -> None:
        pass

    def live_agents(self) -> set[str]:
        return self.live

    def teammates(self, agent: str) -> set[str] | None:
        return self.team

    def report_activity(self, agent: str, report: ActivityReport) -> None:
        pass

    def statuses(self, agent: str) -> dict[str, Status] | None:
        return None


def test_peers_on_a_team_are_the_teammates_only(store: Store, alice: Bus) -> None:
    alice.send("bob", "one")
    alice.send("carol", "two")
    on_team = Bus(store, "alice", registry=TeamRegistry({"alice", "bob", "dave"}, {"dave"}))
    peers = {p.name: p for p in on_team.peers()}
    assert set(peers) == {"bob", "dave"}
    assert peers["bob"].unread_by_them == 1
    assert peers["dave"].live
    assert not peers["bob"].live


def test_a_store_other_than_the_local_one_needs_a_registry(store: Store) -> None:
    class Mirror:
        def __getattr__(self, name: str) -> object:
            return getattr(store, name)

    with pytest.raises(TypeError, match="needs its own registry"):
        Bus(Mirror(), "alice")  # type: ignore[arg-type]


def test_send_returns_the_event_as_the_store_kept_it(store: Store) -> None:
    class Sequencing(Store):
        def append[E: Event](self, event: E) -> E:
            return super().append(cast(E, event.model_copy(update={"seq": 7})))

    sequenced = Sequencing(store.bus_dir)
    message = Bus(sequenced, "alice", registry=Registry(sequenced)).send("bob", "x")
    assert message.seq == 7


def test_check_reply_is_the_chain_rule(alice: Bus) -> None:
    result = MessageEvent(sender="bob", to="alice", kind="result", body="r", thread="T")
    check_reply(result, "task")
    with pytest.raises(BusError, match="ends its chain"):
        check_reply(result, "text")
    check_reply(MessageEvent(sender="bob", to="alice", body="t", thread="T"), "text")
