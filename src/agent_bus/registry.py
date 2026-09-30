"""Which server process currently holds the channel for each agent.

One file per agent under `sessions/`. A record is live only while its
heartbeat is fresh AND its pid still exists: a crashed server leaves a
fresh-looking file behind, and a recycled pid alone would look alive.
Claims are serialized per agent with a lock file, so two sessions starting
together cannot both win.
"""

from __future__ import annotations

import fcntl
import os
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, ValidationError

from agent_bus.models import now
from agent_bus.store import Store

HEARTBEAT_SECONDS = 10
STALE_AFTER = timedelta(seconds=3 * HEARTBEAT_SECONDS)


class SessionRecord(BaseModel):
    agent: str
    session: str
    pid: int
    started: datetime
    heartbeat: datetime


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class Registry:
    def __init__(self, store: Store) -> None:
        self.store = store

    def _path(self, agent: str) -> Path:
        return self.store.sessions_dir / f"{agent}.json"

    @contextmanager
    def _locked(self, agent: str) -> Generator[None]:
        self.store.ensure()
        fd = os.open(self.store.sessions_dir / f"{agent}.lock", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def read(self, agent: str) -> SessionRecord | None:
        try:
            with self._path(agent).open(encoding="utf-8") as fh:
                return SessionRecord.model_validate_json(fh.read())
        except (FileNotFoundError, ValidationError):
            return None

    def _write(self, record: SessionRecord) -> None:
        path = self._path(record.agent)
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(record.model_dump_json())
        tmp.replace(path)

    @staticmethod
    def is_live(record: SessionRecord) -> bool:
        return now() - record.heartbeat < STALE_AFTER and pid_alive(record.pid)

    def claim(self, agent: str, session: str, pid: int) -> bool:
        """Take the agent's channel unless another live session holds it."""
        with self._locked(agent):
            current = self.read(agent)
            if current and current.session != session and self.is_live(current):
                return False
            stamp = now()
            started = current.started if current and current.session == session else stamp
            self._write(
                SessionRecord(
                    agent=agent, session=session, pid=pid, started=started, heartbeat=stamp
                )
            )
            return True

    def heartbeat(self, agent: str, session: str) -> bool:
        """Refresh our record; False if the channel now belongs to another session."""
        with self._locked(agent):
            current = self.read(agent)
            if current is None or current.session != session:
                return False
            self._write(current.model_copy(update={"heartbeat": now()}))
            return True

    def release(self, agent: str, session: str) -> None:
        with self._locked(agent):
            current = self.read(agent)
            if current and current.session == session:
                self._path(agent).unlink()

    def live(self) -> dict[str, SessionRecord]:
        records: dict[str, SessionRecord] = {}
        if not self.store.sessions_dir.exists():
            return records
        for path in sorted(self.store.sessions_dir.glob("*.json")):
            record = self.read(path.stem)
            if record and self.is_live(record):
                records[record.agent] = record
        return records
