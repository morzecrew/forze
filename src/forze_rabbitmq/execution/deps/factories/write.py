"""RabbitMQ queue write dep factory."""

from typing import Any, final

import attrs

from forze.application.contracts.queue import QueueCommandDepPort, QueueSpec
from forze.application.execution import ExecutionContext

from ....adapters import RabbitMQQueueAdapter, RabbitMQQueueCodec
from ..configs import RabbitMQQueueConfig
from ..keys import RabbitMQClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableRabbitMQQueueWrite(QueueCommandDepPort):
    """Configurable RabbitMQ queue command adapter."""

    config: RabbitMQQueueConfig = attrs.field(
        validator=attrs.validators.instance_of(RabbitMQQueueConfig),
    )
    """Configuration for the queue."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: QueueSpec[Any],
    ) -> RabbitMQQueueAdapter[Any]:
        client = ctx.deps.provide(RabbitMQClientDepKey)
        codec = RabbitMQQueueCodec(payload_codec=spec.codec)

        return RabbitMQQueueAdapter(
            client=client,
            codec=codec,
            namespace=self.config.namespace,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
            delayed_delivery=self.config.delayed_delivery,
        )
