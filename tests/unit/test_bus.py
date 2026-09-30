from __future__ import annotations

import pytest

from agent_bus.bus import MAX_BODY_BYTES, Bus, BusError, TurnLimitError
from agent_bus.models import AckEvent


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


def test_turn_limit_counts_the_pair_across_threads(alice: Bus, bob: Bus) -> None:
    for i in range(2):
        alice.send("bob", f"a{i}")
        bob.send("alice", f"b{i}")
    with pytest.raises(TurnLimitError, match="await_human"):
        alice.send("bob", "a fresh thread does not dodge the limit")


def test_await_human_is_never_refused_and_a_human_turn_resets(alice: Bus, bob: Bus) -> None:
    for i in range(2):
        alice.send("bob", f"a{i}")
        bob.send("alice", f"b{i}")
    handoff = alice.send("bob", "waiting for a human", kind="await_human")
    assert handoff.kind == "await_human"
    with pytest.raises(TurnLimitError):
        bob.send("alice", "still over the limit")
    bob.record_human_turn("claude-session")
    assert bob.send("alice", "a human spoke, so this goes through").kind == "text"


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
