"""Lifecycle hooks for Temporal client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import RoutedTemporalClient, TemporalClient, TemporalConfig
from .deps import TemporalClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TemporalStartupHook(LifecycleHook):
    """Startup hook that initializes the Temporal client from the deps container."""

    host: str
    """Connection host for the Temporal server."""

    config: TemporalConfig = TemporalConfig()
    """Configuration for the Temporal client."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        temporal_client = cast(TemporalClient, ctx.dep(TemporalClientDepKey))
        await temporal_client.initialize(self.host, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TemporalShutdownHook(LifecycleHook):
    """Shutdown hook that releases the Temporal client reference."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        temporal_client = ctx.dep(TemporalClientDepKey)
        await temporal_client.close()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedTemporalStartupHook(LifecycleHook):
    """Startup hook that marks a :class:`RoutedTemporalClient` as ready."""

    client: RoutedTemporalClient

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.startup()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedTemporalShutdownHook(LifecycleHook):
    """Shutdown hook that closes all per-tenant Temporal clients."""

    client: RoutedTemporalClient

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.close()


# ....................... #


def temporal_lifecycle_step(
    name: str = "temporal_lifecycle",
    *,
    host: str,
    config: TemporalConfig = TemporalConfig(),
) -> LifecycleStep:
    """Build a lifecycle step for Temporal client init and shutdown."""
    startup_hook = TemporalStartupHook(host=host, config=config)

    return LifecycleStep(
        name=name,
        startup=startup_hook,
        shutdown=TemporalShutdownHook(),
    )


# ....................... #


def routed_temporal_lifecycle_step(
    name: str = "routed_temporal_lifecycle",
    *,
    client: RoutedTemporalClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedTemporalClient` registered as :data:`TemporalClientDepKey`.

    Do not combine with :func:`temporal_lifecycle_step` on the same instance.
    """

    return LifecycleStep(
        name=name,
        startup=RoutedTemporalStartupHook(client=client),
        shutdown=RoutedTemporalShutdownHook(client=client),
    )
