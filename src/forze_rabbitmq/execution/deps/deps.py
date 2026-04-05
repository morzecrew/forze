"""Factory functions for RabbitMQ queue adapters."""

from typing import Any, final

import attrs

from forze.application.contracts.queue import (
    QueueCommandDepPort,
    QueueQueryDepPort,
    QueueSpec,
)
from forze.application.execution import ExecutionContext

from ...adapters import RabbitMQQueueAdapter, RabbitMQQueueCodec
from .configs import RabbitMQQueueConfig
from .keys import RabbitMQClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableRabbitMQQueueRead(QueueQueryDepPort):
    """Configurable RabbitMQ queue query adapter."""

    config: RabbitMQQueueConfig
    """Configuration for the queue."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: QueueSpec[Any],
    ) -> RabbitMQQueueAdapter[Any]:
        client = ctx.dep(RabbitMQClientDepKey)
        codec = RabbitMQQueueCodec(model=spec.model)

        return RabbitMQQueueAdapter(
            client=client,
            codec=codec,
            namespace=self.config.get("namespace"),
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenant_id,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableRabbitMQQueueWrite(QueueCommandDepPort):
    """Configurable RabbitMQ queue command adapter."""

    config: RabbitMQQueueConfig
    """Configuration for the queue."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: QueueSpec[Any],
    ) -> RabbitMQQueueAdapter[Any]:
        client = ctx.dep(RabbitMQClientDepKey)
        codec = RabbitMQQueueCodec(model=spec.model)

        return RabbitMQQueueAdapter(
            client=client,
            codec=codec,
            namespace=self.config.get("namespace"),
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenant_id,
        )
