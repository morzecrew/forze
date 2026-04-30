"""Lifecycle hooks for Mongo client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import MongoClient, MongoConfig, RoutedMongoClient
from .deps import MongoClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoStartupHook(LifecycleHook):
    """Startup hook that initializes the Mongo client from the deps container."""

    uri: str
    """Connection URI for the Mongo database."""

    db_name: str
    """Database name passed to :meth:`MongoClient.initialize`."""

    config: MongoConfig = MongoConfig()
    """Pool configuration for the client."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        mongo_client = cast(MongoClient, ctx.dep(MongoClientDepKey))
        await mongo_client.initialize(
            self.uri, db_name=self.db_name, config=self.config
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoShutdownHook(LifecycleHook):
    """Shutdown hook that closes the Mongo client."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        mongo_client = ctx.dep(MongoClientDepKey)
        await mongo_client.close()


# ....................... #


def mongo_lifecycle_step(
    name: str = "mongo_lifecycle",
    *,
    uri: str,
    db_name: str,
    config: MongoConfig = MongoConfig(),
) -> LifecycleStep:
    """Build a lifecycle step for Mongo client init and shutdown."""
    startup_hook = MongoStartupHook(uri=uri, db_name=db_name, config=config)
    shutdown_hook = MongoShutdownHook()

    return LifecycleStep(name=name, startup=startup_hook, shutdown=shutdown_hook)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedMongoStartupHook(LifecycleHook):
    """Startup hook that marks a :class:`RoutedMongoClient` as ready."""

    client: RoutedMongoClient

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.startup()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedMongoShutdownHook(LifecycleHook):
    """Shutdown hook that closes all per-tenant Mongo clients."""

    client: RoutedMongoClient

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.close()


# ....................... #


def routed_mongo_lifecycle_step(
    name: str = "mongo_routed_lifecycle",
    *,
    client: RoutedMongoClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedMongoClient` registered as :data:`MongoClientDepKey`.

    Do not combine with :func:`mongo_lifecycle_step` on the same client instance.
    """

    return LifecycleStep(
        name=name,
        startup=RoutedMongoStartupHook(client=client),
        shutdown=RoutedMongoShutdownHook(client=client),
    )
