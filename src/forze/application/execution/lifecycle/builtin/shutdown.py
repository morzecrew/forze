"""Generic client-pool shutdown lifecycle hook."""

from typing import TYPE_CHECKING, Any

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class ClientShutdownHook(LifecycleHook):
    """Resolve a client from the deps container and close it on shutdown.

    Integration packages bind :attr:`dep_key` (and, for ``aclose()``-style
    clients, :attr:`close_method`) by subclassing; the resolve-and-close logic
    lives here once instead of being repeated in every package's lifecycle module.
    """

    dep_key: DepKey[Any]
    """Deps key the client was registered under."""

    close_method: str = "close"
    """Async teardown method name (e.g. ``"aclose"`` for httpx/Meilisearch)."""

    # ....................... #

    async def __call__(self, ctx: "ExecutionContext") -> None:
        client = ctx.deps.provide(self.dep_key)

        await getattr(client, self.close_method)()
