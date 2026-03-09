"""Lifecycle hooks for RabbitMQ client initialization and shutdown."""

from typing import final

import attrs

from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep

from ..kernel.platform import RabbitMQConfig
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
        rabbitmq_client = ctx.dep(RabbitMQClientDepKey)
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
