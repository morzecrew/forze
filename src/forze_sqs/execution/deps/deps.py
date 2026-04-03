"""Factory functions for SQS queue adapters."""

from typing import Any, final

import attrs

from forze.application.contracts.queue import (
    QueueReadDepPort,
    QueueSpec,
    QueueWriteDepPort,
)
from forze.application.execution import ExecutionContext

from ...adapters import SQSQueueAdapter, SQSQueueCodec
from .configs import SQSQueueConfig
from .keys import SQSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableSQSQueueRead(QueueReadDepPort):
    """Configurable SQS queue read adapter."""

    config: SQSQueueConfig
    """Configuration for the queue."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: QueueSpec[Any],
    ) -> SQSQueueAdapter[Any]:
        client = ctx.dep(SQSClientDepKey)
        codec = SQSQueueCodec(model=spec.model)

        return SQSQueueAdapter(
            client=client,
            codec=codec,
            namespace=self.config.get("namespace"),
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenant_id,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableSQSQueueWrite(QueueWriteDepPort):
    """Configurable SQS queue write adapter."""

    config: SQSQueueConfig
    """Configuration for the queue."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: QueueSpec[Any],
    ) -> SQSQueueAdapter[Any]:
        client = ctx.dep(SQSClientDepKey)
        codec = SQSQueueCodec(model=spec.model)

        return SQSQueueAdapter(
            client=client,
            codec=codec,
            namespace=self.config.get("namespace"),
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenant_id,
        )
