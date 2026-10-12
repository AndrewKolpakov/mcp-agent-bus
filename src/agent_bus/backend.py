"""The one place that picks where the bus lives.

Everything else talks to an `EventStore` and a `SessionRegistry`. Today the
only backend is the local directory; a hub backend (PLAN.md, v2) is chosen
here once it exists.
"""

from __future__ import annotations

from dataclasses import dataclass

from agent_bus.config import Config
from agent_bus.registry import Registry, SessionRegistry
from agent_bus.store import EventStore, Store


@dataclass(frozen=True)
class Backend:
    store: EventStore
    registry: SessionRegistry


def open_backend(cfg: Config) -> Backend:
    store = Store(cfg.bus_dir)
    return Backend(store=store, registry=Registry(store))
