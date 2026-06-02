"""Lifecycle hooks for Postgres client initialization and shutdown."""

from typing import cast, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step
from forze.base.serialization.pydantic import pydantic_secret_converter

from ...kernel.client import PostgresClient, PostgresConfig, RoutedPostgresClient
from ..deps import PostgresClientDepKey
from .capabilities import POSTGRES_CLIENT_CAPABILITY

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresStartupHook(LifecycleHook):
    """Startup hook that initializes the Postgres client from the deps container."""

    dsn: SecretStr = attrs.field(converter=pydantic_secret_converter, repr=False)
    """Connection DSN for the Postgres database."""

    config: PostgresConfig = attrs.field(factory=PostgresConfig, repr=False)
    """Pool configuration for the client."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        postgres_client = cast(PostgresClient, ctx.deps.provide(PostgresClientDepKey))
        await postgres_client.initialize(self.dsn, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresShutdownHook(LifecycleHook):
    """Shutdown hook that closes the Postgres client pool.

    Resolves :data:`PostgresClientDepKey` and calls :meth:`PostgresClient.close`.
    """

    async def __call__(self, ctx: ExecutionContext) -> None:
        postgres_client = ctx.deps.provide(PostgresClientDepKey)
        await postgres_client.close()


# ....................... #


def postgres_lifecycle_step(
    name: str = "postgres_lifecycle",
    *,
    dsn: str | SecretStr,
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

    return LifecycleStep(
        id=name,
        startup=startup_hook,
        shutdown=shutdown_hook,
        provides=(POSTGRES_CLIENT_CAPABILITY,),
    )


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

    return attrs.evolve(
        routed_client_lifecycle_step(name, client=client),
        provides=(POSTGRES_CLIENT_CAPABILITY,),
    )
