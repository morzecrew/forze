"""SQS queue write dep factory."""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.queue import (
    QueueCommandDepPort,
    QueueCommandPort,
    QueueSpec,
)
from forze.application.execution import ExecutionContext
from forze.application.execution.crypto import enforce_required_reach
from forze.application.integrations.queue import encrypting_queue_command

from ....adapters import SQSQueueAdapter, SQSQueueCodec
from ..configs import SQSQueueConfig
from ..keys import SQSClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableSQSQueueWrite(QueueCommandDepPort):
    """Configurable SQS queue command adapter."""

    config: SQSQueueConfig = attrs.field(
        validator=attrs.validators.instance_of(SQSQueueConfig),
    )
    """Configuration for the queue."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: QueueSpec[Any],
    ) -> QueueCommandPort[Any]:
        enforce_required_reach(
            ctx.deps, route=str(spec.name), declared=spec.encryption, kind="queue"
        )
        client = ctx.deps.provide(SQSClientDepKey)
        codec = SQSQueueCodec(payload_codec=spec.codec)

        adapter = SQSQueueAdapter(
            client=client,
            codec=codec,
            namespace=self.config.namespace,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
            max_batch_payload_bytes=self.config.max_batch_payload_bytes,
        )
        cipher = (
            ctx.deps.provide(KeyringDepKey)
            if ctx.deps.exists(KeyringDepKey)
            else None
        )
        return encrypting_queue_command(
            adapter, spec, cipher=cipher, tenant_provider=ctx.inv_ctx.get_tenant
        )
