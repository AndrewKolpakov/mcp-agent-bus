"""The plugin files agree with each other and with the code.

Claude Code derives the channel `source` and the tool names from the
manifest; the server instructions, the hooks and the skill hard-code them.
These tests fail when one side changes without the other.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import yaml

from agent_bus import __version__
from agent_bus.names import CHANNEL_SOURCE, PLUGIN_NAME, SERVER_KEY, TOOL_PREFIX
from agent_bus.server import INSTRUCTIONS_ON_BUS, TOOLS

ROOT = Path(__file__).resolve().parents[2]
RUN_SH = ROOT / "scripts" / "run.sh"


def _json(relative: str) -> Any:
    return json.loads((ROOT / relative).read_text())


def test_manifest_names_match_the_code() -> None:
    manifest = _json(".claude-plugin/plugin.json")
    assert manifest["name"] == PLUGIN_NAME
    assert list(manifest["mcpServers"]) == [SERVER_KEY]
    assert [c["server"] for c in manifest["channels"]] == [SERVER_KEY]
    assert manifest["version"] == __version__
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert pyproject["project"]["version"] == __version__


def test_server_env_maps_every_user_config_option() -> None:
    manifest = _json(".claude-plugin/plugin.json")
    env = manifest["mcpServers"][SERVER_KEY]["env"]
    assert env == {
        "AGENT_BUS_NAME": "${user_config.agent_name}",
        "AGENT_BUS_DIR": "${user_config.bus_dir}",
        "AGENT_BUS_MAX_TURNS": "${user_config.max_agent_turns}",
    }
    assert set(manifest["userConfig"]) == {"agent_name", "bus_dir", "max_agent_turns"}
    from agent_bus.config import DEFAULT_MAX_AGENT_TURNS

    turns = manifest["userConfig"]["max_agent_turns"]
    assert turns["default"] == DEFAULT_MAX_AGENT_TURNS
    assert turns["min"] <= turns["default"] <= turns["max"]


def test_marketplace_serves_this_directory_as_the_plugin() -> None:
    marketplace = _json(".claude-plugin/marketplace.json")
    assert [(p["name"], p["source"]) for p in marketplace["plugins"]] == [(PLUGIN_NAME, "./")]


def test_no_project_mcp_config_at_the_plugin_root() -> None:
    # A root .mcp.json would also load as project config while developing here.
    assert not (ROOT / ".mcp.json").exists()


def test_instructions_and_skill_use_the_real_names() -> None:
    assert f'source="{CHANNEL_SOURCE}"' in INSTRUCTIONS_ON_BUS.format(me="x", source=CHANNEL_SOURCE)
    skill = (ROOT / "skills" / "agent-bus" / "SKILL.md").read_text()
    assert f'<channel source="{CHANNEL_SOURCE}"' in skill
    for tool in TOOLS:
        assert f"`{TOOL_PREFIX}{tool.name}`" in skill


def test_skill_frontmatter_parses_as_yaml() -> None:
    # A frontmatter that fails to parse drops the skill without an error.
    text = (ROOT / "skills" / "agent-bus" / "SKILL.md").read_text()
    _, frontmatter, _ = text.split("---\n", 2)
    meta = yaml.safe_load(frontmatter)
    assert meta["name"] == PLUGIN_NAME
    assert f'<channel source="{CHANNEL_SOURCE}">' in meta["description"]


def test_hooks_call_run_sh_with_known_events() -> None:
    from agent_bus.hooks import EVENTS

    config = _json("hooks/hooks.json")["hooks"]
    for groups in config.values():
        for group in groups:
            for hook in group["hooks"]:
                assert hook["command"] == "${CLAUDE_PLUGIN_ROOT}/scripts/run.sh"
                assert hook["args"][0] == "hook"
                assert hook["args"][1] in EVENTS
    assert "SessionEnd" not in config


def test_run_sh_is_executable() -> None:
    assert RUN_SH.stat().st_mode & stat.S_IXUSR


def test_run_sh_skips_hooks_off_the_bus_without_starting_uv(tmp_path: Path) -> None:
    env = {"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)}
    result = subprocess.run(
        [str(RUN_SH), "hook", "stop"], env=env, capture_output=True, text=True, timeout=10
    )
    assert (result.returncode, result.stdout, result.stderr) == (0, "", "")


def test_run_sh_starts_the_cli_through_uv(tmp_path: Path) -> None:
    env = {
        "PATH": os.environ["PATH"],
        "HOME": os.environ["HOME"],
        "CLAUDE_PLUGIN_ROOT": str(ROOT),
        "CLAUDE_PLUGIN_DATA": str(tmp_path / "data"),
    }
    result = subprocess.run(
        [str(RUN_SH), "--version"], env=env, capture_output=True, text=True, timeout=300
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == f"agent-bus {__version__}\n"
    assert (tmp_path / "data" / "venv").is_dir()
