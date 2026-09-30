"""The single entry point every plugin component runs through."""

from __future__ import annotations

import subprocess
import sys

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
