from __future__ import annotations

import os
import subprocess
import sys

from agent_bus.models import now
from agent_bus.registry import STALE_AFTER, Registry
from agent_bus.store import Store


def _dead_pid() -> int:
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def test_second_live_session_cannot_claim(store: Store) -> None:
    registry = Registry(store)
    assert registry.claim("bob", "s1", os.getpid())
    assert not registry.claim("bob", "s2", os.getpid())
    assert registry.claim("bob", "s1", os.getpid())
    assert set(registry.live()) == {"bob"}


def test_stale_heartbeat_frees_the_claim(store: Store) -> None:
    registry = Registry(store)
    registry.claim("bob", "s1", os.getpid())
    record = registry.read("bob")
    assert record is not None
    registry._write(record.model_copy(update={"heartbeat": now() - STALE_AFTER}))
    assert registry.live() == {}
    assert registry.claim("bob", "s2", os.getpid())


def test_dead_pid_frees_the_claim_even_with_a_fresh_heartbeat(store: Store) -> None:
    registry = Registry(store)
    registry.claim("bob", "s1", _dead_pid())
    assert registry.live() == {}
    assert registry.claim("bob", "s2", os.getpid())


def test_heartbeat_and_release_only_touch_our_own_record(store: Store) -> None:
    registry = Registry(store)
    registry.claim("bob", "s1", os.getpid())
    assert registry.heartbeat("bob", "s1")
    assert not registry.heartbeat("bob", "other")
    registry.release("bob", "other")
    assert registry.read("bob") is not None
    registry.release("bob", "s1")
    assert registry.read("bob") is None
    assert not registry.heartbeat("bob", "s1")
