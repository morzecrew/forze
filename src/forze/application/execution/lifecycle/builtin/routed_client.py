from typing import TYPE_CHECKING, Protocol, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


class RoutedClientLifecycle(Protocol):
    """Protocol for tenant-routed clients with explicit startup and shutdown."""

    async def startup(self) -> None: ...

    async def close(self) -> None: ...


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class _RoutedClientStartupHook(LifecycleHook):
    client: RoutedClientLifecycle

    # ....................... #

    async def __call__(self, ctx: "ExecutionContext") -> None:
        await self.client.startup()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class _RoutedClientShutdownHook(LifecycleHook):
    client: RoutedClientLifecycle

    # ....................... #

    async def __call__(self, ctx: "ExecutionContext") -> None:
        await self.client.close()


# ....................... #


def routed_client_lifecycle_step(
    name: str,
    *,
    client: RoutedClientLifecycle,
) -> LifecycleStep:
    """Build startup/shutdown hooks for a tenant-routed client."""

    return LifecycleStep(
        id=name,
        startup=_RoutedClientStartupHook(client=client),
        shutdown=_RoutedClientShutdownHook(client=client),
    )
