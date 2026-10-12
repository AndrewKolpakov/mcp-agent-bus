"""Fakes and fixtures shared by the hub tests: a moto table, a clock, a pusher."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from moto import mock_aws

from agent_bus.hub.app import Hub
from agent_bus.hub.table import Table

ENDPOINT = "https://ws.example/dev"


class Clock:
    def __init__(self) -> None:
        self.t = 1_800_000_000.0

    def __call__(self) -> float:
        return self.t


class RecordingPusher:
    def __init__(self) -> None:
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self.gone: set[str] = set()

    def post(self, endpoint: str, conn: str, payload: dict[str, Any]) -> bool:
        if conn in self.gone:
            return False
        self.posts.append((conn, payload))
        return True

    def seqs(self, conn: str) -> list[int]:
        return [p["seq"] for c, p in self.posts if c == conn and "seq" in p]


@pytest.fixture
def table() -> Iterator[Table]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="eu-central-1")
        client.create_table(
            TableName="hub",
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield Table("hub", client)


@pytest.fixture
def tokens(table: Table) -> dict[str, str]:
    """alice, bob and dave are on team core; carol is on team other with dave."""
    issued = {name: table.create_agent(name) for name in ("alice", "bob", "carol", "dave")}
    for name in ("alice", "bob", "dave"):
        table.add_member("core", name)
    for name in ("carol", "dave"):
        table.add_member("other", name)
    return issued


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def pusher() -> RecordingPusher:
    return RecordingPusher()


@pytest.fixture
def hub(table: Table, tokens: dict[str, str], pusher: RecordingPusher, clock: Clock) -> Hub:
    return Hub(table, pusher, rate_limit=3, rate_window=300, clock=clock)
