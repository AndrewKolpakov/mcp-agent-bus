from __future__ import annotations

import hashlib
import stat
from pathlib import Path

import pytest

from agent_bus.bus import Bus, BusError
from agent_bus.models import MAX_ATTACHMENTS, Attachment
from agent_bus.store import AttachmentError, Store


@pytest.fixture
def diff(tmp_path: Path) -> Path:
    path = tmp_path / "change.diff"
    path.write_text("--- a/x.py\n+++ b/x.py\n@@ -1 +1 @@\n-print(1)\n+print(2)\n")
    return path


def test_a_file_travels_by_hash_and_comes_back_checked(
    store: Store, alice: Bus, bob: Bus, diff: Path
) -> None:
    sent = alice.send("bob", "please review", kind="task", attachments=[diff])
    data = diff.read_bytes()
    assert sent.attachments == (
        Attachment(sha256=hashlib.sha256(data).hexdigest(), name="change.diff", size=len(data)),
    )
    [(attachment, path)] = bob.attachments(sent.id)
    assert path == store.attachments_dir / attachment.sha256
    assert path.read_bytes() == data
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_a_file_changed_after_sending_is_refused(alice: Bus, bob: Bus, diff: Path) -> None:
    sent = alice.send("bob", "review", attachments=[diff])
    [(_, path)] = bob.attachments(sent.id)
    path.write_text("something else")
    with pytest.raises(AttachmentError, match="does not match its sha256"):
        bob.attachments(sent.id)


def test_only_the_parties_may_fetch(store: Store, alice: Bus, diff: Path) -> None:
    sent = alice.send("bob", "review", attachments=[diff])
    with pytest.raises(BusError, match="unknown message id"):
        Bus(store, "carol").attachments(sent.id)


def test_bad_attachments_are_refused_before_anything_is_sent(
    store: Store, alice: Bus, tmp_path: Path, diff: Path
) -> None:
    with pytest.raises(BusError, match="cannot read attachment"):
        alice.send("bob", "x", attachments=[tmp_path / "missing"])
    with pytest.raises(BusError, match="not a regular file"):
        alice.send("bob", "x", attachments=[tmp_path])
    with pytest.raises(BusError, match=f"at most {MAX_ATTACHMENTS}"):
        alice.send("bob", "x", attachments=[diff] * (MAX_ATTACHMENTS + 1))
    assert store.read_all().events == []


def test_a_name_is_never_a_path() -> None:
    for name in ("../x", "a/b", "..", "", "x\ny"):
        with pytest.raises(ValueError, match="plain file name"):
            Attachment(sha256="0" * 64, name=name, size=1)


def test_a_message_without_files_has_no_attachments_field(alice: Bus) -> None:
    sent = alice.send("bob", "plain")
    assert b"attachments" not in sent.model_dump_json(by_alias=True, exclude_none=True).encode()
