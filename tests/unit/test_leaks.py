from __future__ import annotations

from pathlib import Path

import pytest

from agent_bus.bus import Bus, BusError
from agent_bus.leaks import refusal, scan
from agent_bus.store import Store

# Built at run time so that this file holds no secret-shaped literal itself.
HUB_TOKEN = "ab_" + "Zq3" * 15
GITHUB = "ghp_" + "a1B2" * 9
JWT = "eyJhbGciOiJIUzI1NiJ9." + "eyJzdWIiOiIxMjM0NTY3ODkwIn0." + "dozjgNryP4J3jVmNHl0w5N_XgL0n"
AWS_ID = "AKIA" + "IOSFODNN7ABCDEFG"
AWS_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCY" + "z" * 10


@pytest.mark.parametrize(
    ("text", "rule"),
    [
        (AWS_ID, "AWS access key id"),
        (f"aws_secret_access_key = {AWS_SECRET}", "AWS secret access key"),
        ("-----BEGIN OPENSSH " + "PRIVATE KEY-----", "private key"),
        (f"export GITHUB_TOKEN={GITHUB}", "GitHub token"),
        (HUB_TOKEN, "agent-bus hub token"),
        (JWT, "JSON web token"),
        ("Authorization: Bearer " + "abcDEF123456" * 3, "bearer token"),
        ("postgres://app:" + "s3cr3t" + "@db:5432/x", "password in a URL"),
        ("DB_PASSWORD=" + "correcthorse", "a value assigned to 'DB_PASSWORD'"),
        ('login(user, password="' + "hunter2hunter2" + '")', "a value assigned to 'password'"),
        ("secret: " + "s3cretP4ss!", "a value assigned to 'secret'"),
    ],
)
def test_secrets_are_found_by_rule_and_line(text: str, rule: str) -> None:
    findings = scan(f"first line\n{text}\n")
    assert [(f.rule, f.line) for f in findings] == [(rule, 2)]


@pytest.mark.parametrize(
    "text",
    [
        "password=password",
        "token = os.environ['TOKEN']",
        'PASSWORD_FIELD = "password_hash"',
        "TOKEN=${TOKEN}",
        "API_KEY=<your key>",
        "TOKEN_FILE=/home/x/.token",
        "api_key: str = Field(...)",
        "token = read_token(home / TOKEN_NAME)",
        "the password is stored in ~/.config/x",
        "AGENT_BUS_SESSION=1",
        "postgres://app:${PW}@db:5432/x",
        "sha256 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
        "commit 5ebe1034b7a3f0e1c2d9e8f7a6b5c4d3e2f1a0b9",
    ],
)
def test_code_and_placeholders_pass(text: str) -> None:
    assert scan(text) == []


def test_the_refusal_names_rule_and_line_but_never_the_secret() -> None:
    text = refusal(scan(f"ok\n{GITHUB}"))
    assert "body line 2: GitHub token" in text
    assert GITHUB not in text
    assert "Nothing was sent" in text


def test_send_refuses_a_secret_and_writes_nothing(store: Store, alice: Bus) -> None:
    with pytest.raises(BusError, match="line 1: AWS access key id"):
        alice.send("bob", f"{AWS_ID} is the key")
    assert store.read_all().events == []


def test_an_attachment_is_checked_like_a_body(store: Store, alice: Bus, tmp_path: Path) -> None:
    env = tmp_path / "config.env"
    env.write_text("DEBUG=1\nDB_PASSWORD=" + "correcthorse\n")
    with pytest.raises(BusError, match=r"attachment config\.env line 2"):
        alice.send("bob", "please review", attachments=[env])
    assert store.read_all().events == []
    assert not store.attachments_dir.exists()
