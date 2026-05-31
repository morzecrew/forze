"""ClickHouse client pool lifecycle hooks and step factories."""

from typing import cast, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step

from ...kernel.client import ClickHouseClient, ClickHouseConfig, RoutedClickHouseClient
from ..deps import ClickHouseClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ClickHouseStartupHook(LifecycleHook):
    """Startup hook that initializes the ClickHouse client."""

    connection: ClickHouseConfig
    """Connection settings passed to :meth:`ClickHouseClient.initialize`."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        ch_client = cast(ClickHouseClient, ctx.deps.provide(ClickHouseClientDepKey))
        await ch_client.initialize(self.connection)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ClickHouseShutdownHook(LifecycleHook):
    """Shutdown hook that closes the ClickHouse client."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        ch_client = ctx.deps.provide(ClickHouseClientDepKey)
        await ch_client.close()


# ....................... #


def clickhouse_lifecycle_step(
    name: str = "clickhouse_lifecycle",
    *,
    connection: ClickHouseConfig,
) -> LifecycleStep:
    """Build a lifecycle step for ClickHouse client init and shutdown."""

    return LifecycleStep(
        id=name,
        startup=ClickHouseStartupHook(connection=connection),
        shutdown=ClickHouseShutdownHook(),
    )


# ....................... #


def routed_clickhouse_lifecycle_step(
    name: str = "routed_clickhouse_lifecycle",
    *,
    client: RoutedClickHouseClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedClickHouseClient` registered as :data:`ClickHouseClientDepKey`.

    Do not combine with :func:`clickhouse_lifecycle_step` on the same instance.
    """

    return routed_client_lifecycle_step(name, client=client)
