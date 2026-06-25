"""Redis pub-sub dep factories — query (raw) and command (encrypting).

The query side returns the raw adapter; the command side wraps with whole-payload
encryption and enforces the deployment ``required_reach`` floor (mirrors the stream/queue
factories). Pub-sub is at-most-once past the broker — see ``OutboxDestination.pubsub``.
"""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.pubsub import (
    PubSubCommandPort,
    PubSubQueryPort,
    PubSubSpec,
)
from forze.application.contracts.pubsub.deps import (
    PubSubCommandDepPort,
    PubSubQueryDepPort,
)
from forze.application.execution import ExecutionContext
from forze.application.execution.crypto import enforce_required_reach
from forze.application.integrations.pubsub import encrypting_pubsub_command

from ....adapters import RedisPubSubAdapter, RedisPubSubCodec
from ..configs import RedisPubSubConfig
from ..keys import RedisClientDepKey

# ----------------------- #


def _pubsub_adapter(
    ctx: ExecutionContext, spec: PubSubSpec[Any], config: RedisPubSubConfig
) -> RedisPubSubAdapter[Any]:
    return RedisPubSubAdapter(
        client=ctx.deps.provide(RedisClientDepKey),
        codec=RedisPubSubCodec(payload_codec=spec.codec),
        tenant_aware=config.tenant_aware,
        tenant_provider=ctx.inv_ctx.get_tenant,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisPubSubQuery(PubSubQueryDepPort):
    """Build a subscribe-only :class:`RedisPubSubAdapter` (``PubSubQueryPort``)."""

    config: RedisPubSubConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisPubSubConfig),
    )

    def __call__(
        self, ctx: ExecutionContext, spec: PubSubSpec[Any]
    ) -> PubSubQueryPort[Any]:
        return _pubsub_adapter(ctx, spec, self.config)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisPubSubCommand(PubSubCommandDepPort):
    """Build a publish :class:`RedisPubSubAdapter`, encryption-wrapped per ``spec``."""

    config: RedisPubSubConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisPubSubConfig),
    )

    def __call__(
        self, ctx: ExecutionContext, spec: PubSubSpec[Any]
    ) -> PubSubCommandPort[Any]:
        enforce_required_reach(
            ctx.deps,
            route=str(spec.name),
            declared=spec.encryption,
            kind="pubsub",
            supports_at_rest=False,
        )
        adapter = _pubsub_adapter(ctx, spec, self.config)
        cipher = (
            ctx.deps.provide(KeyringDepKey)
            if ctx.deps.exists(KeyringDepKey)
            else None
        )
        return encrypting_pubsub_command(
            adapter, spec, cipher=cipher, tenant_provider=ctx.inv_ctx.get_tenant
        )
