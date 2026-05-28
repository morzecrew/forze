"""Lifecycle hooks for Redis client initialization and shutdown."""

from typing import cast, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step
from forze.base.serialization import pydantic_secret_converter

from ..kernel.platform import RedisClient, RedisConfig, RoutedRedisClient
from .deps import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisStartupHook(LifecycleHook):
    """Startup hook that initializes the Redis client from the deps container.

    Resolves :data:`RedisClientDepKey` and calls :meth:`RedisClient.initialize`
    with the DSN and config. The client must be registered before startup runs.
    """

    dsn: SecretStr = attrs.field(converter=pydantic_secret_converter, repr=False)
    """Connection DSN or URL for the Redis instance."""

    config: RedisConfig = attrs.field(factory=RedisConfig, repr=False)
    """Connection pool configuration for the client."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        redis_client = cast(RedisClient, ctx.deps.provide(RedisClientDepKey))
        await redis_client.initialize(self.dsn, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisShutdownHook(LifecycleHook):
    """Shutdown hook that closes the Redis client connection pool.

    Resolves :data:`RedisClientDepKey` and calls :meth:`RedisClient.close`.
    """

    async def __call__(self, ctx: ExecutionContext) -> None:
        redis_client = ctx.deps.provide(RedisClientDepKey)
        await redis_client.close()


# ....................... #


def redis_lifecycle_step(
    name: str = "redis_lifecycle",
    *,
    dsn: str | SecretStr,
    config: RedisConfig = RedisConfig(),
) -> LifecycleStep:
    """Build a lifecycle step for Redis client init and shutdown.

    :param name: Step name for collision detection.
    :param dsn: Connection DSN or URL.
    :param config: Pool configuration.
    :returns: Lifecycle step with startup and shutdown hooks.
    """
    startup_hook = RedisStartupHook(dsn=dsn, config=config)
    shutdown_hook = RedisShutdownHook()

    return LifecycleStep(id=name, startup=startup_hook, shutdown=shutdown_hook)


# ....................... #


def routed_redis_lifecycle_step(
    name: str = "routed_redis_lifecycle",
    *,
    client: RoutedRedisClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedRedisClient` registered as :data:`RedisClientDepKey`.

    Do not combine with :func:`redis_lifecycle_step` on the same instance.
    """

    return routed_client_lifecycle_step(name, client=client)
