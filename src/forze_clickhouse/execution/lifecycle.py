"""Lifecycle hooks for ClickHouse client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext

from ..kernel.platform import ClickHouseClient, ClickHouseConfig
from .deps import ClickHouseClientDepKey

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
