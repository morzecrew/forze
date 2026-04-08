"""Lifecycle hooks for Postgres client initialization and shutdown."""

from typing import final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import PostgresConfig
from .deps import PostgresClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresStartupHook(LifecycleHook):
    """Startup hook that initializes the Postgres client from the deps container."""

    dsn: str
    """Connection DSN for the Postgres database."""

    config: PostgresConfig = PostgresConfig()
    """Pool configuration for the client."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        postgres_client = ctx.dep(PostgresClientDepKey)
        await postgres_client.initialize(self.dsn, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresShutdownHook(LifecycleHook):
    """Shutdown hook that closes the Postgres client pool.

    Resolves :data:`PostgresClientDepKey` and calls :meth:`PostgresClient.close`.
    """

    async def __call__(self, ctx: ExecutionContext) -> None:
        postgres_client = ctx.dep(PostgresClientDepKey)
        await postgres_client.close()


# ....................... #


def postgres_lifecycle_step(
    name: str = "postgres_lifecycle",
    *,
    dsn: str,
    config: PostgresConfig = PostgresConfig(),
) -> LifecycleStep:
    """Build a lifecycle step for Postgres client init and shutdown.

    :param name: Step name for collision detection.
    :param dsn: Connection DSN.
    :param config: Pool configuration.
    :returns: Lifecycle step with startup and shutdown hooks.
    """

    startup_hook = PostgresStartupHook(dsn=dsn, config=config)
    shutdown_hook = PostgresShutdownHook()

    return LifecycleStep(name=name, startup=startup_hook, shutdown=shutdown_hook)
