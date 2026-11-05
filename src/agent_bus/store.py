"""The on-disk bus: an append-only JSONL event log plus a session registry dir.

Every writer, the MCP server and the hook processes alike, appends through
`Store.append`: an exclusive `flock` and a single `write` per line on an
O_APPEND descriptor, so concurrent writers from several processes never
interleave inside a line. Readers take no lock and parse only up to the last
newline, so a line still being written is never half-read.

Permissions are the authentication: the directory is 0700 and the log 0600,
so only this OS user can put text in front of an agent.

Attachments sit beside the log in `attachments/`, one file per content,
named by its sha256 (models.Attachment).

`EventStore` is what the bus, the server and the hooks need from a store.
`Store` is the local implementation. A hub store (PLAN.md, v2) appends over
HTTP and reads a local mirror of the agent's view, a JSONL file in this same
format, so byte offsets and `ReadResult` mean the same for both.
"""

from __future__ import annotations

import fcntl
import hashlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from pydantic import ValidationError

from agent_bus.models import EVENT_ADAPTER, Attachment, Event, MessageEvent, dump_event

log = logging.getLogger(__name__)

LOG_NAME = "log.jsonl"
SESSIONS_DIR = "sessions"
ATTACHMENTS_DIR = "attachments"


class AttachmentError(ValueError):
    """An attachment that is missing or not what its message says; safe to show the model."""


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def verified(path: Path, attachment: Attachment) -> bool:
    """Whether the file at `path` is exactly the attachment: its size and its hash."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return False
    return len(data) == attachment.size and sha256_of(data) == attachment.sha256


def write_private(directory: Path, name: str, data: bytes) -> Path:
    """Write `data` to `directory/name`, 0600, atomically; `name` is always a hash here."""
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = directory / name
    tmp = directory / f".{name}.{os.getpid()}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
    tmp.replace(path)
    return path


@dataclass(frozen=True)
class ReadResult:
    events: list[Event]
    offset: int
    """Byte offset just past the last complete line that was parsed."""
    skipped: int
    """Complete lines that were not valid events (unknown type, bad JSON)."""


class EventStore(Protocol):
    def ensure(self) -> None: ...

    def append[E: Event](self, event: E) -> E:
        """Write `event`; return it as stored, which on a hub carries `seq`."""
        ...

    def require_thread_posts(self) -> None:
        """Raise `BusError` before anything is posted when this store would ignore `thread`."""
        ...

    def size(self) -> int: ...

    def read_from(self, offset: int = 0) -> ReadResult: ...

    def read_all(self) -> ReadResult: ...

    def put_attachment(self, attachment: Attachment, data: bytes) -> None:
        """Keep `data` where the message's recipient can fetch it."""
        ...

    def attachment_path(self, message: MessageEvent, attachment: Attachment) -> Path:
        """A local file holding exactly `attachment`, fetched first if need be.

        Raises `AttachmentError` when it cannot be had or does not match
        its hash: a file that differs from what was sent is never handed on.
        """
        ...


class Store:
    def __init__(self, bus_dir: Path) -> None:
        self.bus_dir = bus_dir
        self.log_path = bus_dir / LOG_NAME
        self.sessions_dir = bus_dir / SESSIONS_DIR
        self.attachments_dir = bus_dir / ATTACHMENTS_DIR

    def require_thread_posts(self) -> None:
        """The local log always honours `thread`."""

    def ensure(self) -> None:
        self.bus_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.sessions_dir.mkdir(mode=0o700, exist_ok=True)

    def append[E: Event](self, event: E) -> E:
        self.ensure()
        line = dump_event(event)
        fd = os.open(self.log_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                written = os.write(fd, line)
                if written != len(line):
                    raise OSError(f"short write to {self.log_path}: {written}/{len(line)} bytes")
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        return event

    def size(self) -> int:
        try:
            return self.log_path.stat().st_size
        except FileNotFoundError:
            return 0

    def read_from(self, offset: int = 0) -> ReadResult:
        try:
            with self.log_path.open("rb") as fh:
                fh.seek(offset)
                chunk = fh.read()
        except FileNotFoundError:
            return ReadResult(events=[], offset=offset, skipped=0)
        complete, newline, _partial = chunk.rpartition(b"\n")
        if not newline:
            return ReadResult(events=[], offset=offset, skipped=0)
        events: list[Event] = []
        skipped = 0
        for raw in complete.split(b"\n"):
            if not raw.strip():
                continue
            try:
                events.append(EVENT_ADAPTER.validate_json(raw))
            except ValidationError:
                skipped += 1
                log.warning("skipping unreadable log line: %.200r", raw)
        return ReadResult(events=events, offset=offset + len(complete) + 1, skipped=skipped)

    def read_all(self) -> ReadResult:
        return self.read_from(0)

    def put_attachment(self, attachment: Attachment, data: bytes) -> None:
        if not verified(self.attachments_dir / attachment.sha256, attachment):
            self.ensure()
            write_private(self.attachments_dir, attachment.sha256, data)

    def attachment_path(self, message: MessageEvent, attachment: Attachment) -> Path:
        path = self.attachments_dir / attachment.sha256
        if not path.exists():
            raise AttachmentError(f"attachment {attachment.name} of {message.id} is missing")
        if not verified(path, attachment):
            raise AttachmentError(
                f"attachment {attachment.name} of {message.id} does not match its sha256; "
                "it was changed after it was sent, so do not use it"
            )
        return path
