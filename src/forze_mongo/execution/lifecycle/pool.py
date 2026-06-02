"""Mongo client pool lifecycle hooks and step factories."""

from typing import cast, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step
from forze.base.serialization.pydantic import pydantic_secret_converter

from ...kernel.client import MongoClient, MongoConfig, RoutedMongoClient
from ..deps import MongoClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoStartupHook(LifecycleHook):
    """Startup hook that initializes the Mongo client from the deps container."""

    uri: SecretStr = attrs.field(converter=pydantic_secret_converter, repr=False)
    """Connection URI for the Mongo database."""

    db_name: str
    """Database name passed to :meth:`MongoClient.initialize`."""

    config: MongoConfig = attrs.field(factory=MongoConfig, repr=False)
    """Pool configuration for the client."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        mongo_client = cast(MongoClient, ctx.deps.provide(MongoClientDepKey))
        await mongo_client.initialize(
            self.uri,
            db_name=self.db_name,
            config=self.config,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoShutdownHook(LifecycleHook):
    """Shutdown hook that closes the Mongo client."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        mongo_client = ctx.deps.provide(MongoClientDepKey)
        await mongo_client.close()


# ....................... #


def mongo_lifecycle_step(
    name: str = "mongo_lifecycle",
    *,
    uri: str | SecretStr,
    db_name: str,
    config: MongoConfig = MongoConfig(),
) -> LifecycleStep:
    """Build a lifecycle step for Mongo client init and shutdown."""
    startup_hook = MongoStartupHook(uri=uri, db_name=db_name, config=config)
    shutdown_hook = MongoShutdownHook()

    return LifecycleStep(id=name, startup=startup_hook, shutdown=shutdown_hook)


# ....................... #


def routed_mongo_lifecycle_step(
    name: str = "routed_mongo_lifecycle",
    *,
    client: RoutedMongoClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedMongoClient` registered as :data:`MongoClientDepKey`.

    Do not combine with :func:`mongo_lifecycle_step` on the same client instance.
    """

    return routed_client_lifecycle_step(name, client=client)
