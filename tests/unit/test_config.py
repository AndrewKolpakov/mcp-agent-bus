from __future__ import annotations

from pathlib import Path

import pytest

from agent_bus import config


def test_defaults() -> None:
    cfg = config.load({})
    assert cfg.bus_dir == Path("~/.agent-bus").expanduser()
    assert cfg.agent is None
    assert (cfg.rate_limit, cfg.rate_window) == (
        config.DEFAULT_RATE_LIMIT,
        config.DEFAULT_RATE_WINDOW_SECONDS,
    )
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
        "CLAUDE_PLUGIN_OPTION_RATE_LIMIT": "8.0",
        "CLAUDE_PLUGIN_OPTION_RATE_WINDOW_SECONDS": "60",
    }
    cfg = config.load(env)
    assert cfg.bus_dir == Path("~/bus").expanduser()
    assert cfg.agent == "bob"
    assert (cfg.rate_limit, cfg.rate_window) == (8, 60.0)


def test_session_override_wins_over_bus_name_and_plugin_option() -> None:
    env = {
        "AGENT_BUS_AS": "carol",
        "AGENT_BUS_NAME": "alice",
        "CLAUDE_PLUGIN_OPTION_AGENT_NAME": "bob",
    }
    assert config.load(env).agent == "carol"
    assert config.load({**env, "AGENT_BUS_AS": " "}).agent == "alice"


def test_session_override_is_validated() -> None:
    with pytest.raises(ValueError, match="invalid agent name"):
        config.load({"AGENT_BUS_AS": "Bad Name", "AGENT_BUS_NAME": "alice"})


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
        config.load({"AGENT_BUS_RATE_LIMIT": "0"})
    with pytest.raises(ValueError, match="positive"):
        config.load({"AGENT_BUS_RATE_WINDOW": "0"})
    with pytest.raises(ValueError, match="not set"):
        config.load({}).require_agent()


def test_hub_comes_from_env_then_plugin_option() -> None:
    assert config.load({}).hub is None
    assert (
        config.load({"CLAUDE_PLUGIN_OPTION_HUB_URL": "https://hub.example/"}).hub
        == "https://hub.example"
    )
    both = {
        "AGENT_BUS_HUB": "https://a.example",
        "CLAUDE_PLUGIN_OPTION_HUB_URL": "https://b.example",
    }
    assert config.load(both).hub == "https://a.example"
    assert config.load({"AGENT_BUS_HUB": "${user_config.hub_url}"}).hub is None


def test_hub_override_beats_the_plugin_setting() -> None:
    # The manifest sets AGENT_BUS_HUB from hub_url, empty or not; AGENT_BUS_VIA
    # comes from the shell and wins over it, and an argument wins over both.
    env = {"AGENT_BUS_VIA": "https://via.example/", "AGENT_BUS_HUB": "https://a.example"}
    assert config.load(env).hub == "https://via.example"
    assert config.load({**env, "AGENT_BUS_VIA": " "}).hub == "https://a.example"
    assert config.load(env, hub="https://arg.example").hub == "https://arg.example"
    with pytest.raises(ValueError, match="https://"):
        config.load({"AGENT_BUS_VIA": "http://via.example"})


@pytest.mark.parametrize("url", ["http://hub.example", "hub.example", "https://"])
def test_hub_must_be_https(url: str) -> None:
    with pytest.raises(ValueError, match="https://"):
        config.load({"AGENT_BUS_HUB": url})


def test_channel_is_claude_unless_set() -> None:
    assert config.load({}).channel == "claude"
    assert config.load({"AGENT_BUS_CHANNEL": "0"}).channel == "inbox"
    assert config.load({"AGENT_BUS_CHANNEL": "codex"}).channel == "codex"
    with pytest.raises(ValueError, match="AGENT_BUS_CHANNEL"):
        config.load({"AGENT_BUS_CHANNEL": "no"})
