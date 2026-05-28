"""Lifecycle hooks for Inngest client readiness."""

from typing import final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step

from ..kernel.platform import RoutedInngestClient

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class InngestStartupHook(LifecycleHook):
    """Startup hook that validates the Inngest client is registered."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        from .deps import InngestClientDepKey

        _ = ctx.deps.provide(InngestClientDepKey)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class InngestShutdownHook(LifecycleHook):
    """Shutdown hook (no-op; Inngest SDK client needs no explicit teardown)."""

    async def __call__(self, _ctx: ExecutionContext) -> None:
        return


# ....................... #


def inngest_lifecycle_step(name: str = "inngest_lifecycle") -> LifecycleStep:
    """Build a lifecycle step for Inngest client registration checks."""

    return LifecycleStep(
        id=name,
        startup=InngestStartupHook(),
        shutdown=InngestShutdownHook(),
    )


def routed_inngest_lifecycle_step(
    name: str = "routed_inngest_lifecycle",
    *,
    client: RoutedInngestClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedInngestClient` registered as :data:`InngestClientDepKey`."""

    return routed_client_lifecycle_step(name, client=client)
