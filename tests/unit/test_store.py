from __future__ import annotations

import multiprocessing
import stat
from pathlib import Path

from agent_bus.models import AckEvent, MessageEvent
from agent_bus.store import Store


def _msg(body: str = "hi") -> MessageEvent:
    return MessageEvent(sender="alice", to="bob", body=body, thread="T")


def test_append_then_read_all(store: Store) -> None:
    first, second = _msg("one"), AckEvent(id="X", by="bob")
    store.append(first)
    store.append(second)
    result = store.read_all()
    assert result.events == [first, second]
    assert result.offset == store.size()
    assert result.skipped == 0


def test_missing_log_reads_empty(store: Store) -> None:
    result = store.read_all()
    assert result.events == []
    assert result.offset == 0


def test_directory_and_log_are_private(store: Store) -> None:
    store.append(_msg())
    assert stat.S_IMODE(store.bus_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(store.log_path.stat().st_mode) == 0o600


def test_partial_trailing_line_is_left_for_later(store: Store) -> None:
    store.append(_msg("complete"))
    complete_size = store.size()
    with store.log_path.open("ab") as fh:
        fh.write(b'{"type": "ack", "id": "X"')
    result = store.read_all()
    assert [e.body for e in result.events if isinstance(e, MessageEvent)] == ["complete"]
    assert result.offset == complete_size
    with store.log_path.open("ab") as fh:
        fh.write(b', "by": "bob"}\n')
    tail = store.read_from(result.offset)
    assert len(tail.events) == 1
    ack = tail.events[0]
    assert isinstance(ack, AckEvent)
    assert (ack.id, ack.by) == ("X", "bob")
    assert tail.offset == store.size()


def test_read_from_returns_only_new_events(store: Store) -> None:
    store.append(_msg("old"))
    offset = store.read_all().offset
    store.append(_msg("new"))
    result = store.read_from(offset)
    assert [e.body for e in result.events if isinstance(e, MessageEvent)] == ["new"]


def test_unreadable_lines_are_skipped_and_counted(store: Store) -> None:
    store.append(_msg("before"))
    with store.log_path.open("ab") as fh:
        fh.write(b"not json\n")
        fh.write(b'{"type": "from-the-future", "v": 9}\n')
    store.append(_msg("after"))
    result = store.read_all()
    assert [e.body for e in result.events if isinstance(e, MessageEvent)] == ["before", "after"]
    assert result.skipped == 2


def _writer(bus_dir: str, worker: int, count: int) -> None:
    store = Store(Path(bus_dir))
    for i in range(count):
        store.append(
            MessageEvent(sender="alice", to="bob", body=f"{worker}-{i}-" + "x" * 3000, thread="T")
        )


def test_concurrent_appends_from_processes_never_interleave(tmp_path: Path) -> None:
    bus_dir = tmp_path / "bus"
    workers, per_worker = 4, 150
    ctx = multiprocessing.get_context("spawn")
    procs = [
        ctx.Process(target=_writer, args=(str(bus_dir), w, per_worker)) for w in range(workers)
    ]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(timeout=60)
        assert proc.exitcode == 0
    result = Store(bus_dir).read_all()
    assert result.skipped == 0
    assert len(result.events) == workers * per_worker
    labels = {e.body.split("-x")[0] for e in result.events if isinstance(e, MessageEvent)}
    assert labels == {f"{w}-{i}" for w in range(workers) for i in range(per_worker)}
