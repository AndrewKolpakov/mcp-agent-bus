from __future__ import annotations

from pathlib import Path

import pytest

from agent_bus import config


def test_defaults() -> None:
    cfg = config.load({})
    assert cfg.bus_dir == Path("~/.agent-bus").expanduser()
    assert cfg.agent is None
    assert cfg.max_agent_turns == config.DEFAULT_MAX_AGENT_TURNS
    assert cfg.on_bus is False


def test_bus_env_wins_over_plugin_options() -> None:
    env = {
        "AGENT_BUS_DIR": "/tmp/a",
        "CLAUDE_PLUGIN_OPTION_BUS_DIR": "/tmp/b",
        "AGENT_BUS_NAME": "alice",
        "CLAUDE_PLUGIN_OPTION_AGENT_NAME": "bob",
    }
    cfg = config.load(env)
    assert (cfg.bus_dir, cfg.agent) == (Path("/tmp/a"), "alice")


def test_plugin_options_expand_home_and_parse_numbers() -> None:
    env = {
        "CLAUDE_PLUGIN_OPTION_BUS_DIR": "~/bus",
        "CLAUDE_PLUGIN_OPTION_AGENT_NAME": "bob",
        "CLAUDE_PLUGIN_OPTION_MAX_AGENT_TURNS": "8.0",
    }
    cfg = config.load(env)
    assert cfg.bus_dir == Path("~/bus").expanduser()
    assert cfg.agent == "bob"
    assert cfg.max_agent_turns == 8


def test_empty_and_unsubstituted_values_count_as_unset() -> None:
    env = {"AGENT_BUS_NAME": "", "CLAUDE_PLUGIN_OPTION_AGENT_NAME": "${user_config.agent_name}"}
    assert config.load(env).agent is None


def test_arguments_win_over_environment() -> None:
    cfg = config.load({"AGENT_BUS_NAME": "alice"}, agent="carol", bus_dir="/tmp/x")
    assert (cfg.agent, cfg.bus_dir) == ("carol", Path("/tmp/x"))


@pytest.mark.parametrize(
    ("value", "expected"), [("1", True), (" 1 ", True), ("0", False), ("yes", False)]
)
def test_session_opt_in_is_exactly_one(value: str, expected: bool) -> None:
    assert config.load({"AGENT_BUS_SESSION": value}).on_bus is expected


def test_bad_values_are_rejected() -> None:
    with pytest.raises(ValueError, match="invalid agent name"):
        config.load({"AGENT_BUS_NAME": "Bad Name"})
    with pytest.raises(ValueError, match="at least 1"):
        config.load({"AGENT_BUS_MAX_TURNS": "0"})
    with pytest.raises(ValueError, match="not set"):
        config.load({}).require_agent()
