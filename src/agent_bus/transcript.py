"""Read a Claude Code session transcript to confirm channel deliveries.

Claude Code accepts a channel notification without acknowledging it and
drops it silently when the session cannot take it: while the connection is
still being set up, in a background fork that lost the channels flag, or in
a foreground process whose conversation moved to the background (stage 5
live runs). The one trace a delivered event leaves is a record in the
session transcript: a `user` record with `origin.kind == "channel"` whose
content is the `<channel ...>` tag carrying the msg_id.

The transcript format is internal to Claude Code. The file is found by
session id under `$CLAUDE_CONFIG_DIR/projects/*/`, without deriving the
project directory name, and a missing file means "cannot confirm", which the
server treats as the old behaviour rather than as a deaf session.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from agent_bus.names import CHANNEL_SOURCE

MSG_ID = re.compile(r'msg_id="([0-9A-HJKMNP-TV-Z]{26})"')


def channel_ids(record: object) -> set[str]:
    """msg_ids of this bus's channel events in one transcript record."""
    if not isinstance(record, dict):
        return set()
    origin = record.get("origin")
    if not isinstance(origin, dict) or origin.get("kind") != "channel":
        return set()
    if origin.get("server") not in (None, CHANNEL_SOURCE):
        return set()
    message = record.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    if isinstance(content, list):
        content = " ".join(str(part.get("text", "")) for part in content if isinstance(part, dict))
    if not isinstance(content, str) or f'source="{CHANNEL_SOURCE}"' not in content:
        return set()
    return set(MSG_ID.findall(content))


class Transcript:
    def __init__(self, config_dir: Path, session: str) -> None:
        self.config_dir = config_dir
        self.session = session
        self._path: Path | None = None
        self._offset = 0

    def path(self) -> Path | None:
        if self._path is None:
            matches = sorted((self.config_dir / "projects").glob(f"*/{self.session}.jsonl"))
            self._path = matches[0] if matches else None
        return self._path

    def new_channel_ids(self) -> set[str]:
        """msg_ids of channel events written since the previous call."""
        path = self.path()
        if path is None:
            return set()
        try:
            with path.open("rb") as fh:
                fh.seek(self._offset)
                chunk = fh.read()
        except FileNotFoundError:
            return set()
        complete, newline, _ = chunk.rpartition(b"\n")
        if not newline:
            return set()
        self._offset += len(complete) + 1
        found: set[str] = set()
        for raw in complete.split(b"\n"):
            if b'"channel"' not in raw:
                continue
            try:
                found |= channel_ids(json.loads(raw))
            except json.JSONDecodeError:
                continue
        return found
