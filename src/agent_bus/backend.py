"""The one place that picks where the bus lives.

Everything else talks to an `EventStore` and a `SessionRegistry`: the local
directory, or a hub when `Config.hub` is set (PLAN.md, v2). The hub client
is imported only in that branch, so the local bus never loads httpx or
websockets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from agent_bus.config import Config
from agent_bus.registry import Registry, SessionRegistry
from agent_bus.store import EventStore, Store

if TYPE_CHECKING:
    from agent_bus.remote import HubApi, HubLink

HOOK_TIMEOUT_SECONDS = 5.0
"""A hook's hub call must outlast a Lambda cold start, about 2 s (stage 7, Q10)."""


@dataclass(frozen=True)
class Backend:
    store: EventStore
    registry: SessionRegistry
    api: HubApi | None = None
    link: HubLink | None = None
    """The hub socket, for a server to run; None on the local bus."""


def open_backend(cfg: Config, *, timeout: float = 15.0) -> Backend:
    if cfg.hub is None:
        store = Store(cfg.bus_dir)
        return Backend(store=store, registry=Registry(store))

    from agent_bus.remote import (
        TOKEN_NAME,
        HubApi,
        HubLink,
        HubRegistry,
        HubStore,
        hub_home,
        read_token,
    )

    home = hub_home(cfg.bus_dir, cfg.hub, cfg.require_agent())
    token = read_token(home / TOKEN_NAME)
    api = HubApi(cfg.hub, token, timeout=timeout)
    hub_store = HubStore(api, home)
    registry = HubRegistry(api)
    return Backend(
        store=hub_store,
        registry=registry,
        api=api,
        link=HubLink("", token, hub_store, registry),
    )
