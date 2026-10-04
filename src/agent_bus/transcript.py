"""Read a Claude Code session transcript to confirm channel deliveries.

Claude Code accepts a channel notification without acknowledging it and
drops it silently when the session cannot take it: while the connection is
still being set up, in a background fork started without the channels
flag, and in a foreground process whose conversation moved to the
background (stage 5 live runs). The transcript is the only trace.

An event enters the session's input queue and is taken out of it when the
conversation consumes it. What the transcript shows (Claude Code 2.1.286,
counted over every local transcript):

- `queue-operation` `enqueue`, with the `<channel ...>` tag as `content`.
  A process whose conversation moved to the background still writes this
  and nothing after it, so an enqueue alone confirms nothing.
- `queue-operation` `dequeue`, without content: the oldest queued item
  starts a turn (an idle session).
- `queue-operation` `remove` with content and `reason` `absorbed_mid_turn`
  or `delivered_to_agent`: a busy session took the item into its turn; an
  `attachment` of type `queued_command` carries the same tag.
- `queue-operation` `popAll`: the queue was emptied without a turn.
- A `user` record with `origin.kind == "channel"` and the tag as content.

A msg_id is confirmed by a dequeue of its queue entry, a delivering remove,
a `queued_command` attachment, or a channel `user` record. The format is
internal to Claude Code; the file is found by session id under
`$CLAUDE_CONFIG_DIR/projects/*/`.
"""

from __future__ import annotations

import json
import re
from collections import deque
from pathlib import Path
from typing import Any

from agent_bus.names import CHANNEL_SOURCE

MSG_ID = re.compile(r'msg_id="([0-9A-HJKMNP-TV-Z]{26})"')
DELIVERING_REMOVALS = frozenset({"absorbed_mid_turn", "delivered_to_agent"})


def _text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return " ".join(str(p.get("text", "")) for p in value if isinstance(p, dict))
    return ""


def bus_ids(text: str) -> set[str]:
    """msg_ids in the `<channel>` tags of this bus within some text."""
    if f'source="{CHANNEL_SOURCE}"' not in text:
        return set()
    return set(MSG_ID.findall(text))


def channel_ids(record: object) -> set[str]:
    """msg_ids a single record shows as consumed by the conversation."""
    if not isinstance(record, dict):
        return set()
    if record.get("type") == "attachment":
        attachment = record.get("attachment")
        if isinstance(attachment, dict) and attachment.get("type") == "queued_command":
            return bus_ids(_text(attachment.get("prompt")))
        return set()
    origin = record.get("origin")
    if not isinstance(origin, dict) or origin.get("kind") != "channel":
        return set()
    if origin.get("server") not in (None, CHANNEL_SOURCE):
        return set()
    message = record.get("message")
    return bus_ids(_text(message.get("content") if isinstance(message, dict) else None))


def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return 0.0


class Transcript:
    def __init__(self, config_dir: Path, session: str) -> None:
        self.config_dir = config_dir
        self.session = session
        self._path: Path | None = None
        self._offset = 0
        self._queue: deque[set[str]] = deque()
        """msg_ids of each queued item, oldest first (empty for non-bus items)."""

    def path(self) -> Path | None:
        """The session's transcript, looked up afresh on every call.

        Claude Code moves a session's transcript to another project folder
        when the session changes directory, for example on entering a git
        worktree; the old file then stops growing (stage-5 follow-up,
        2026-10-04: a cached path left a live session deaf for 32 hours).
        The most recently written match is the live one.
        """
        matches = list((self.config_dir / "projects").glob(f"*/{self.session}.jsonl"))
        if not matches:
            return self._path
        newest = max(matches, key=_mtime)
        if newest != self._path:
            # Records already read from the old file reappear in the new one;
            # confirming an id twice is harmless, missing one is not.
            self._path, self._offset = newest, 0
            self._queue.clear()
        return self._path

    def layout_known(self) -> bool:
        """Whether any session transcript exists where this one is looked for."""
        return next((self.config_dir / "projects").glob("*/*.jsonl"), None) is not None

    def _apply(self, record: dict[str, Any]) -> set[str]:
        if record.get("type") != "queue-operation":
            return channel_ids(record)
        operation = record.get("operation")
        ids = bus_ids(_text(record.get("content")))
        if operation == "enqueue":
            self._queue.append(ids)
        elif operation == "dequeue":
            return self._queue.popleft() if self._queue else set()
        elif operation == "remove":
            for index, queued in enumerate(self._queue):
                if queued == ids:
                    del self._queue[index]
                    break
            if record.get("reason") in DELIVERING_REMOVALS:
                return ids
        elif operation == "popAll":
            self._queue.clear()
        return set()

    def new_channel_ids(self) -> set[str]:
        """msg_ids the conversation consumed since the previous call."""
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
            if (
                b"queue-operation" not in raw
                and b'"channel"' not in raw
                and b"queued_command" not in raw
            ):
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict):
                found |= self._apply(record)
        return found
