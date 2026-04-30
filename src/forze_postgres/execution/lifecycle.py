"""Lifecycle hooks for Postgres client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import PostgresClient, PostgresConfig, RoutedPostgresClient
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
        postgres_client = cast(PostgresClient, ctx.dep(PostgresClientDepKey))
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


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedPostgresStartupHook(LifecycleHook):
    """Startup hook that marks a :class:`RoutedPostgresClient` as ready."""

    client: RoutedPostgresClient
    """The same instance registered under :data:`PostgresClientDepKey`."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.startup()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedPostgresShutdownHook(LifecycleHook):
    """Shutdown hook that closes all per-tenant pools on a routed client."""

    client: RoutedPostgresClient
    """The same instance registered under :data:`PostgresClientDepKey`."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.close()


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


# ....................... #


def routed_postgres_lifecycle_step(
    name: str = "routed_postgres_lifecycle",
    *,
    client: RoutedPostgresClient,
) -> LifecycleStep:
    """Build a lifecycle step for tenant-routed Postgres (secrets-backed DSNs).

    Use with :class:`RoutedPostgresClient` registered as :data:`PostgresClientDepKey`.
    Do not use :func:`postgres_lifecycle_step` together with a routed client.

    :param name: Step name for collision detection.
    :param client: Routed client instance (shared with the deps module).
    :returns: Lifecycle step with startup and shutdown hooks.
    """

    return LifecycleStep(
        name=name,
        startup=RoutedPostgresStartupHook(client=client),
        shutdown=RoutedPostgresShutdownHook(client=client),
    )
