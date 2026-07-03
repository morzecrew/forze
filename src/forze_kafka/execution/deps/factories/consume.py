"""Kafka consume dep factory (``CommitStreamGroupQueryPort``)."""

from typing import Any, final

import attrs

from forze.application.contracts.stream import CommitStreamGroupQueryPort, StreamSpec
from forze.application.contracts.stream.deps import CommitStreamGroupQueryDepPort
from forze.application.execution import ExecutionContext
from forze.application.execution.crypto import enforce_required_reach

from ....adapters import KafkaCommitStreamGroupAdapter, KafkaStreamCodec
from ..configs import KafkaCommitStreamGroupConfig
from ..keys import KafkaClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableKafkaConsume(CommitStreamGroupQueryDepPort):
    """Build a :class:`KafkaCommitStreamGroupAdapter` (offset-log read/commit)."""

    config: KafkaCommitStreamGroupConfig = attrs.field(
        validator=attrs.validators.instance_of(KafkaCommitStreamGroupConfig),
    )

    def __call__(
        self, ctx: ExecutionContext, spec: StreamSpec[Any]
    ) -> CommitStreamGroupQueryPort[Any]:
        enforce_required_reach(
            ctx.deps,
            route=str(spec.name),
            declared=spec.encryption,
            kind="stream",
            supports_at_rest=False,
        )
        return KafkaCommitStreamGroupAdapter(
            client=ctx.deps.provide(KafkaClientDepKey),
            codec=KafkaStreamCodec(payload_codec=spec.codec),
            namespace=self.config.namespace,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
            auto_offset_reset=self.config.auto_offset_reset,
            max_poll_records=self.config.max_poll_records,
        )
