"""The single entry point every plugin component runs through."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from agent_bus import __version__
from agent_bus.cli import main


def test_version_flag_prints_the_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"agent-bus {__version__}"


def test_no_command_prints_help_and_signals_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 2
    assert "usage: agent-bus" in capsys.readouterr().out


def test_module_entry_point_runs_the_same_cli() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "agent_bus", "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == f"agent-bus {__version__}"


def _run(bus_dir: Path, *argv: str) -> int:
    return main(["--bus-dir", str(bus_dir), *argv])


def test_send_inbox_ack_round_trip(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert _run(tmp_path, "--as", "alice", "send", "--to", "bob", "--kind", "task", "do it") == 0
    msg_id = capsys.readouterr().out.strip()
    assert len(msg_id) == 26

    assert _run(tmp_path, "--as", "bob", "inbox", "--json") == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert [json.loads(line)["id"] for line in lines] == [msg_id]

    assert _run(tmp_path, "--as", "bob", "ack", msg_id) == 0
    assert capsys.readouterr().out.strip() == f"{msg_id} acked"
    assert _run(tmp_path, "--as", "bob", "inbox") == 0
    assert capsys.readouterr().out == ""
    assert _run(tmp_path, "--as", "bob", "inbox", "--all") == 0
    assert "alice -> bob [task]" in capsys.readouterr().out


def test_send_reads_body_from_stdin(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "stdin", __import__("io").StringIO("from stdin\nline two"))
    assert _run(tmp_path, "--as", "alice", "send", "--to", "bob", "-") == 0
    capsys.readouterr()
    assert _run(tmp_path, "--as", "bob", "inbox", "--json") == 0
    assert json.loads(capsys.readouterr().out)["body"] == "from stdin\nline two"


def test_peers_and_tail(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _run(tmp_path, "--as", "alice", "send", "--to", "bob", "hi")
    capsys.readouterr()
    assert _run(tmp_path, "--as", "alice", "peers", "--json") == 0
    assert json.loads(capsys.readouterr().out) == {
        "name": "bob",
        "live": False,
        "unread_by_them": 1,
    }
    assert _run(tmp_path, "tail") == 0
    out = capsys.readouterr().out
    assert "alice -> bob [text]" in out
    assert "    hi" in out


def test_errors_go_to_stderr_with_exit_1(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("AGENT_BUS_AS", raising=False)
    monkeypatch.delenv("AGENT_BUS_NAME", raising=False)
    monkeypatch.delenv("CLAUDE_PLUGIN_OPTION_AGENT_NAME", raising=False)
    assert _run(tmp_path, "--as", "alice", "send", "--to", "alice", "x") == 1
    assert "yourself" in capsys.readouterr().err
    assert _run(tmp_path, "inbox") == 1
    assert "agent name is not set" in capsys.readouterr().err


def test_codex_wrapper_puts_only_this_session_on_the_bus(tmp_path: Path) -> None:
    from agent_bus import config
    from agent_bus.cli import codex_argv

    cfg = config.load(
        {"AGENT_BUS_NAME": "codex", "AGENT_BUS_HUB": "https://hub.example"},
        bus_dir=str(tmp_path),
    )
    argv = codex_argv(cfg, "/bin/codex", ["--", "-m", "gpt-x"])
    assert argv[:2] == ["/bin/codex", "--no-daemon"]
    overrides = dict(argv[i + 1].split("=", 1) for i, a in enumerate(argv) if a == "-c")
    env = overrides["mcp_servers.agent-bus.env"]
    for pair in (
        'AGENT_BUS_SESSION = "1"',
        'AGENT_BUS_CHANNEL = "codex"',
        'AGENT_BUS_NAME = "codex"',
        'AGENT_BUS_HUB = "https://hub.example"',
        'AGENT_BUS_CODEX_BIN = "/bin/codex"',
    ):
        assert pair in env
    assert overrides["mcp_servers.agent-bus.args"] == '["-m", "agent_bus", "serve"]'
    assert argv[-3:-1] == ["-m", "gpt-x"]
    assert 'agent "codex"' in argv[-1]


def test_claude_wrapper_keeps_the_environment_and_loads_the_channel(tmp_path: Path) -> None:
    out = tmp_path / "claude-run.json"
    fake = tmp_path / "bin" / "claude"
    fake.parent.mkdir()
    fake.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        f"json.dump({{'argv': sys.argv[1:], 'env': dict(os.environ)}}, open({str(out)!r}, 'w'))\n"
    )
    fake.chmod(0o755)
    venv_bin = str(Path(sys.prefix) / "bin")
    env = {
        "PATH": os.pathsep.join([venv_bin, str(fake.parent), "/usr/bin", "/bin"]),
        "HOME": str(tmp_path),
        "CLAUDE_CONFIG_DIR": "/accounts/second",
        "VIRTUAL_ENV": sys.prefix,
        "UV_PROJECT_ENVIRONMENT": sys.prefix,
    }
    done = subprocess.run(
        [
            sys.executable,
            "-m",
            "agent_bus",
            "--as",
            "zed",
            "--hub",
            "https://hub.example/",
            "claude",
            "--",
            "--model",
            "sonnet",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert done.returncode == 0, done.stderr
    run = json.loads(out.read_text())
    assert run["argv"] == [
        "--allowedTools",
        "mcp__plugin_agent-bus_bus",
        "--dangerously-load-development-channels",
        "plugin:agent-bus@agent-bus-dev",
        "--model",
        "sonnet",
    ]
    assert run["env"]["CLAUDE_CONFIG_DIR"] == "/accounts/second"
    assert run["env"]["AGENT_BUS_SESSION"] == "1"
    assert run["env"]["AGENT_BUS_AS"] == "zed"
    assert run["env"]["AGENT_BUS_VIA"] == "https://hub.example"
    assert "VIRTUAL_ENV" not in run["env"]
    assert "UV_PROJECT_ENVIRONMENT" not in run["env"]
    assert venv_bin not in run["env"]["PATH"].split(os.pathsep)


def test_session_env_keeps_what_is_not_ours() -> None:
    from agent_bus.cli import session_env

    env = session_env(
        {"VIRTUAL_ENV": "/elsewhere/venv", "PATH": "/a:/b", "CLAUDE_CONFIG_DIR": "/x"}
    )
    assert env == {"VIRTUAL_ENV": "/elsewhere/venv", "PATH": "/a:/b", "CLAUDE_CONFIG_DIR": "/x"}
