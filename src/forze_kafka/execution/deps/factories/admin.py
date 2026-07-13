"""Kafka admin dep factory (``CommitStreamGroupAdminPort``)."""

from typing import Any, final

import attrs

from forze.application.contracts.stream import CommitStreamGroupAdminPort, StreamSpec
from forze.application.contracts.stream.deps import CommitStreamGroupAdminDepPort
from forze.application.execution import ExecutionContext

from ....adapters import KafkaCommitStreamGroupAdminAdapter
from ..configs import KafkaCommitStreamGroupConfig
from ..keys import KafkaClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableKafkaAdmin(CommitStreamGroupAdminDepPort):
    """Build a :class:`KafkaCommitStreamGroupAdminAdapter` (topic/group/replay/lag)."""

    config: KafkaCommitStreamGroupConfig = attrs.field(
        validator=attrs.validators.instance_of(KafkaCommitStreamGroupConfig),
    )

    def __call__(self, ctx: ExecutionContext, spec: StreamSpec[Any]) -> CommitStreamGroupAdminPort:
        return KafkaCommitStreamGroupAdminAdapter(
            client=ctx.deps.provide(KafkaClientDepKey),
            namespace=self.config.namespace,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
