"""Lifecycle hooks for RabbitMQ client initialization and shutdown."""

from typing import cast, final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import RabbitMQClient, RabbitMQConfig, RoutedRabbitMQClient
from .deps import RabbitMQClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RabbitMQStartupHook(LifecycleHook):
    """Startup hook that initializes the RabbitMQ client from the deps container."""

    dsn: str
    config: RabbitMQConfig = RabbitMQConfig()

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        rabbitmq_client = cast(RabbitMQClient, ctx.dep(RabbitMQClientDepKey))
        await rabbitmq_client.initialize(self.dsn, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RabbitMQShutdownHook(LifecycleHook):
    """Shutdown hook that closes the RabbitMQ connection."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        rabbitmq_client = ctx.dep(RabbitMQClientDepKey)
        await rabbitmq_client.close()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedRabbitMQStartupHook(LifecycleHook):
    """Startup hook that marks a :class:`RoutedRabbitMQClient` as ready."""

    client: RoutedRabbitMQClient

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.startup()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RoutedRabbitMQShutdownHook(LifecycleHook):
    """Shutdown hook that closes all per-tenant RabbitMQ connections."""

    client: RoutedRabbitMQClient

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await self.client.close()


# ....................... #


def rabbitmq_lifecycle_step(
    name: str = "rabbitmq_lifecycle",
    *,
    dsn: str,
    config: RabbitMQConfig = RabbitMQConfig(),
) -> LifecycleStep:
    """Build a lifecycle step for RabbitMQ client init and shutdown."""
    startup_hook = RabbitMQStartupHook(dsn=dsn, config=config)
    shutdown_hook = RabbitMQShutdownHook()

    return LifecycleStep(name=name, startup=startup_hook, shutdown=shutdown_hook)


# ....................... #


def routed_rabbitmq_lifecycle_step(
    name: str = "routed_rabbitmq_lifecycle",
    *,
    client: RoutedRabbitMQClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedRabbitMQClient` registered as :data:`RabbitMQClientDepKey`.

    Do not combine with :func:`rabbitmq_lifecycle_step` on the same instance.
    """

    return LifecycleStep(
        name=name,
        startup=RoutedRabbitMQStartupHook(client=client),
        shutdown=RoutedRabbitMQShutdownHook(client=client),
    )
