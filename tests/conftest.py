from __future__ import annotations

from pathlib import Path

import pytest

from agent_bus.bus import Bus
from agent_bus.store import Store


@pytest.fixture
def store(tmp_path: Path) -> Store:
    return Store(tmp_path / "bus")


@pytest.fixture
def alice(store: Store) -> Bus:
    return Bus(store, "alice", rate_limit=4)


@pytest.fixture
def bob(store: Store) -> Bus:
    return Bus(store, "bob", rate_limit=4)
