"""Kafka produce dep factory (``StreamCommandPort``, encryption-wrapped)."""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.stream import StreamCommandPort, StreamSpec
from forze.application.contracts.stream.deps import StreamCommandDepPort
from forze.application.execution import ExecutionContext
from forze.application.execution.crypto import enforce_required_reach
from forze.application.integrations.stream import encrypting_stream_command

from ....adapters import KafkaStreamCodec, KafkaStreamCommandAdapter
from ..configs import KafkaStreamConfig
from ..keys import KafkaClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableKafkaProduce(StreamCommandDepPort):
    """Build an append :class:`KafkaStreamCommandAdapter`, encryption-wrapped per ``spec``."""

    config: KafkaStreamConfig = attrs.field(
        validator=attrs.validators.instance_of(KafkaStreamConfig),
    )

    def __call__(
        self, ctx: ExecutionContext, spec: StreamSpec[Any]
    ) -> StreamCommandPort[Any]:
        enforce_required_reach(
            ctx.deps,
            route=str(spec.name),
            declared=spec.encryption,
            kind="stream",
            supports_at_rest=False,
        )
        adapter = KafkaStreamCommandAdapter(
            client=ctx.deps.provide(KafkaClientDepKey),
            codec=KafkaStreamCodec(payload_codec=spec.codec),
            namespace=self.config.namespace,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
        cipher = (
            ctx.deps.provide(KeyringDepKey) if ctx.deps.exists(KeyringDepKey) else None
        )
        return encrypting_stream_command(
            adapter, spec, cipher=cipher, tenant_provider=ctx.inv_ctx.get_tenant
        )
