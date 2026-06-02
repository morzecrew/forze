"""RabbitMQ client pool lifecycle hooks and step factories."""

from typing import cast, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle.builtin import routed_client_lifecycle_step
from forze.base.serialization.pydantic import pydantic_secret_converter

from ...kernel.client import RabbitMQClient, RabbitMQConfig, RoutedRabbitMQClient
from ..deps import RabbitMQClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RabbitMQStartupHook(LifecycleHook):
    """Startup hook that initializes the RabbitMQ client from the deps container."""

    dsn: SecretStr = attrs.field(converter=pydantic_secret_converter, repr=False)
    config: RabbitMQConfig = attrs.field(factory=RabbitMQConfig, repr=False)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        rabbitmq_client = cast(RabbitMQClient, ctx.deps.provide(RabbitMQClientDepKey))
        await rabbitmq_client.initialize(self.dsn, config=self.config)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RabbitMQShutdownHook(LifecycleHook):
    """Shutdown hook that closes the RabbitMQ connection."""

    async def __call__(self, ctx: ExecutionContext) -> None:
        rabbitmq_client = ctx.deps.provide(RabbitMQClientDepKey)
        await rabbitmq_client.close()


# ....................... #


def rabbitmq_lifecycle_step(
    name: str = "rabbitmq_lifecycle",
    *,
    dsn: str | SecretStr,
    config: RabbitMQConfig = RabbitMQConfig(),
) -> LifecycleStep:
    """Build a lifecycle step for RabbitMQ client init and shutdown."""
    startup_hook = RabbitMQStartupHook(dsn=dsn, config=config)
    shutdown_hook = RabbitMQShutdownHook()

    return LifecycleStep(id=name, startup=startup_hook, shutdown=shutdown_hook)


# ....................... #


def routed_rabbitmq_lifecycle_step(
    name: str = "routed_rabbitmq_lifecycle",
    *,
    client: RoutedRabbitMQClient,
) -> LifecycleStep:
    """Lifecycle for :class:`RoutedRabbitMQClient` registered as :data:`RabbitMQClientDepKey`.

    Do not combine with :func:`rabbitmq_lifecycle_step` on the same instance.
    """

    return routed_client_lifecycle_step(name, client=client)
