"""Runtime configuration, resolved from arguments then environment.

Precedence for each value: explicit argument, then `AGENT_BUS_*`, then the
plugin's `CLAUDE_PLUGIN_OPTION_*` (exported to hooks), then the default. An
empty value or an unsubstituted `${...}` placeholder counts as unset.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from agent_bus.models import check_agent_name

DEFAULT_BUS_DIR = "~/.agent-bus"
DEFAULT_MAX_AGENT_TURNS = 600
SESSION_OPT_IN = "AGENT_BUS_SESSION"


@dataclass(frozen=True)
class Config:
    bus_dir: Path
    agent: str | None
    max_agent_turns: int
    on_bus: bool
    """True only for a session launched with AGENT_BUS_SESSION=1."""

    def require_agent(self) -> str:
        if self.agent is None:
            raise ValueError(
                "agent name is not set: pass --as, or set AGENT_BUS_NAME or the plugin's agent_name"
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
    raw_agent = agent or _pick(env, "AGENT_BUS_NAME", "CLAUDE_PLUGIN_OPTION_AGENT_NAME")
    raw_turns = _pick(env, "AGENT_BUS_MAX_TURNS", "CLAUDE_PLUGIN_OPTION_MAX_AGENT_TURNS")
    turns = DEFAULT_MAX_AGENT_TURNS
    if raw_turns is not None:
        turns = int(float(raw_turns))
        if turns < 1:
            raise ValueError(f"max agent turns must be at least 1, got {raw_turns!r}")
    return Config(
        bus_dir=Path(raw_dir or DEFAULT_BUS_DIR).expanduser(),
        agent=check_agent_name(raw_agent) if raw_agent else None,
        max_agent_turns=turns,
        on_bus=env.get(SESSION_OPT_IN, "").strip() == "1",
    )
