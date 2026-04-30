"""Lifecycle hooks for Redis client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

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

    dsn: str
    """Connection DSN or URL for the Redis instance."""

    config: RedisConfig = RedisConfig()
    """Connection pool configuration for the client."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        redis_client = cast(RedisClient, ctx.dep(RedisClientDepKey))
        await redis_client.initialize(self.dsn, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisShutdownHook(LifecycleHook):
    """Shutdown hook that closes the Redis client connection pool.

    Resolves :data:`RedisClientDepKey` and calls :meth:`RedisClient.close`.
    """

    async def __call__(self, ctx: ExecutionContext) -> None:
        redis_client = ctx.dep(RedisClientDepKey)
        await redis_client.close()


# ....................... #


def redis_lifecycle_step(
    name: str = "redis_lifecycle",
    *,
    dsn: str,
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

    return LifecycleStep(name=name, startup=startup_hook, shutdown=shutdown_hook)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedRedisStartupHook(LifecycleHook):
    """Startup hook that marks a :class:`RoutedRedisClient` as ready."""

    client: RoutedRedisClient

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.startup()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedRedisShutdownHook(LifecycleHook):
    """Shutdown hook that closes all per-tenant Redis clients."""

    client: RoutedRedisClient

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.close()


# ....................... #


def routed_redis_lifecycle_step(
    name: str = "redis_routed_lifecycle",
    *,
    client: RoutedRedisClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedRedisClient` registered as :data:`RedisClientDepKey`.

    Do not combine with :func:`redis_lifecycle_step` on the same instance.
    """

    return LifecycleStep(
        name=name,
        startup=RoutedRedisStartupHook(client=client),
        shutdown=RoutedRedisShutdownHook(client=client),
    )
