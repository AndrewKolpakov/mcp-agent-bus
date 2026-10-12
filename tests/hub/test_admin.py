"""`agent-bus admin` against a moto table."""

from __future__ import annotations

import pytest
from hubfakes import RecordingPusher, table

from agent_bus import admin
from agent_bus.cli import build_parser
from agent_bus.hub.app import Hub, HubError
from agent_bus.hub.table import Table

__all__ = ["table"]  # fixtures


def run(table: Table, *argv: str) -> int:
    return admin.run(build_parser().parse_args(["admin", *argv]), table)


def test_an_agent_added_to_a_team_can_write_to_teammates(
    table: Table, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(table, "agent", "add", "alice", "--team", "core") == 0
    alice_token = capsys.readouterr().out.strip()
    assert run(table, "agent", "add", "bob") == 0
    capsys.readouterr()
    assert table.agent_for_token(alice_token) == "alice"
    hub = Hub(table, RecordingPusher())
    with pytest.raises(HubError, match="not on any of your teams"):
        hub.post_event("alice", {"to": "bob", "body": "x"})
    assert run(table, "member", "add", "core", "bob") == 0
    assert hub.post_event("alice", {"to": "bob", "body": "x"}).status == 200
    capsys.readouterr()
    run(table, "agent", "list")
    assert capsys.readouterr().out == "alice teams=core\nbob teams=core\n"
    run(table, "team", "list")
    assert capsys.readouterr().out == "core: alice bob\n"


def test_a_token_is_printed_once_and_only_its_hash_is_kept(
    table: Table, capsys: pytest.CaptureFixture[str]
) -> None:
    run(table, "agent", "add", "alice")
    out = capsys.readouterr()
    token = out.out.strip()
    assert token.startswith("ab_")
    assert "shown once" in out.err
    items = table.client.scan(TableName=table.name)["Items"]
    assert token not in repr(items)


def test_rotate_and_revoke_replace_and_drop_the_token(
    table: Table, capsys: pytest.CaptureFixture[str]
) -> None:
    run(table, "agent", "add", "alice")
    first = capsys.readouterr().out.strip()
    run(table, "token", "rotate", "alice")
    second = capsys.readouterr().out.strip()
    assert table.agent_for_token(first) is None
    assert table.agent_for_token(second) == "alice"
    run(table, "token", "revoke", "alice")
    assert table.agent_for_token(second) is None
    run(table, "token", "revoke", "alice")
    assert "had no token" in capsys.readouterr().out
    run(table, "token", "rotate", "alice")
    assert table.agent_for_token(capsys.readouterr().out.strip()) == "alice"


def test_membership_needs_an_existing_agent(
    table: Table, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(table, "member", "add", "core", "ghost") == 1
    assert "no agent 'ghost'" in capsys.readouterr().err
    assert table.members("core") == set()
    assert run(table, "token", "rotate", "ghost") == 1


def test_removing_an_agent_takes_its_view_so_a_new_one_cannot_read_it(
    table: Table, capsys: pytest.CaptureFixture[str]
) -> None:
    run(table, "agent", "add", "alice", "--team", "core")
    run(table, "agent", "add", "bob", "--team", "core")
    capsys.readouterr()
    hub = Hub(table, RecordingPusher())
    hub.post_event("alice", {"to": "bob", "body": "secret"})
    assert run(table, "agent", "remove", "bob") == 1
    assert run(table, "agent", "remove", "bob", "--yes") == 0
    run(table, "agent", "add", "bob")
    capsys.readouterr()
    assert hub.events("bob", {}).body == ""
    assert table.members("core") == {"alice"}
    assert len(hub.events("alice", {}).body.splitlines()) == 1


def test_tail_prints_the_whole_log(table: Table, capsys: pytest.CaptureFixture[str]) -> None:
    run(table, "agent", "add", "alice", "--team", "core")
    run(table, "agent", "add", "bob", "--team", "core")
    hub = Hub(table, RecordingPusher())
    for i in range(3):
        hub.post_event("alice", {"to": "bob", "body": f"m{i}"})
    capsys.readouterr()
    run(table, "tail", "-n", "2")
    out = capsys.readouterr().out
    assert "m1" in out
    assert "m2" in out
    assert "m0" not in out
    assert [e.seq for e in table.log_after(1)] == [2, 3]


def test_a_team_without_members_leaves_the_list(
    table: Table, capsys: pytest.CaptureFixture[str]
) -> None:
    run(table, "agent", "add", "alice", "--team", "core")
    run(table, "member", "remove", "core", "alice")
    capsys.readouterr()
    run(table, "team", "list")
    assert capsys.readouterr().out == ""
