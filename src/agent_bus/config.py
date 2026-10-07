"""Runtime configuration, resolved from arguments then environment.

Precedence for each value: explicit argument, then `AGENT_BUS_*`, then the
plugin's `CLAUDE_PLUGIN_OPTION_*` (exported to hooks), then the default. An
empty value or an unsubstituted `${...}` placeholder counts as unset.

The agent name has one more source ahead of `AGENT_BUS_NAME`: `AGENT_BUS_AS`,
a per-session override from the launching shell. The manifest's `env` block
sets `AGENT_BUS_NAME` for the server from the account's `agent_name`, which
overrides the shell's value, so a shell `AGENT_BUS_NAME` would reach only the
hooks. `AGENT_BUS_AS` is absent from that block and reaches both.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from agent_bus.models import check_agent_name

DEFAULT_RATE_LIMIT = 30
DEFAULT_RATE_WINDOW_SECONDS = 300.0

DEFAULT_BUS_DIR = "~/.agent-bus"
SESSION_OPT_IN = "AGENT_BUS_SESSION"


@dataclass(frozen=True)
class Config:
    bus_dir: Path
    agent: str | None
    rate_limit: int
    rate_window: float
    on_bus: bool
    """True only for a session launched with AGENT_BUS_SESSION=1."""

    def require_agent(self) -> str:
        if self.agent is None:
            raise ValueError(
                "agent name is not set: pass --as, or set AGENT_BUS_AS, AGENT_BUS_NAME"
                " or the plugin's agent_name"
            )
        return self.agent


def _pick(env: Mapping[str, str], *keys: str) -> str | None:
    for key in keys:
        value = env.get(key, "").strip()
        if value and not value.startswith("${"):
            return value
    return None


def load(
    env: Mapping[str, str] | None = None,
    *,
    bus_dir: str | None = None,
    agent: str | None = None,
) -> Config:
    env = os.environ if env is None else env
    raw_dir = bus_dir or _pick(env, "AGENT_BUS_DIR", "CLAUDE_PLUGIN_OPTION_BUS_DIR")
    raw_agent = agent or _pick(
        env, "AGENT_BUS_AS", "AGENT_BUS_NAME", "CLAUDE_PLUGIN_OPTION_AGENT_NAME"
    )
    raw_limit = _pick(env, "AGENT_BUS_RATE_LIMIT", "CLAUDE_PLUGIN_OPTION_RATE_LIMIT")
    raw_window = _pick(env, "AGENT_BUS_RATE_WINDOW", "CLAUDE_PLUGIN_OPTION_RATE_WINDOW_SECONDS")
    limit = DEFAULT_RATE_LIMIT if raw_limit is None else int(float(raw_limit))
    window = DEFAULT_RATE_WINDOW_SECONDS if raw_window is None else float(raw_window)
    if limit < 1:
        raise ValueError(f"rate limit must be at least 1 message, got {raw_limit!r}")
    if window <= 0:
        raise ValueError(f"rate window must be positive, got {raw_window!r}")
    return Config(
        bus_dir=Path(raw_dir or DEFAULT_BUS_DIR).expanduser(),
        agent=check_agent_name(raw_agent) if raw_agent else None,
        rate_limit=limit,
        rate_window=window,
        on_bus=env.get(SESSION_OPT_IN, "").strip() == "1",
    )
