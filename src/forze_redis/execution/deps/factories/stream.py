"""Redis stream dep factories — query, command (encrypting), and group query/admin.

One adapter + config per route, registered under separate dep keys: the query/group/admin
sides return the raw adapter; only the command side wraps with whole-payload encryption and
enforces the deployment ``required_reach`` floor (mirrors SQS read/write and the mock).
"""

from typing import Any, final

import attrs

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.stream import (
    StreamCommandPort,
    StreamGroupAdminPort,
    StreamGroupQueryPort,
    StreamQueryPort,
    StreamSpec,
)
from forze.application.contracts.stream.deps import (
    StreamCommandDepPort,
    StreamGroupAdminDepPort,
    StreamGroupQueryDepPort,
    StreamQueryDepPort,
)
from forze.application.execution import ExecutionContext
from forze.application.execution.crypto import enforce_required_reach
from forze.application.integrations.stream import encrypting_stream_command

from ....adapters import (
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
    RedisStreamGroupAdminAdapter,
)
from ..configs import RedisStreamConfig, RedisStreamGroupConfig
from ..keys import RedisClientDepKey

# ----------------------- #


def _stream_adapter(
    ctx: ExecutionContext, spec: StreamSpec[Any], config: RedisStreamConfig
) -> RedisStreamAdapter[Any]:
    return RedisStreamAdapter(
        client=ctx.deps.provide(RedisClientDepKey),
        codec=RedisStreamCodec(payload_codec=spec.codec),
        tenant_aware=config.tenant_aware,
        tenant_provider=ctx.inv_ctx.get_tenant,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisStreamQuery(StreamQueryDepPort):
    """Build a read-only :class:`RedisStreamAdapter` (``StreamQueryPort``)."""

    config: RedisStreamConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisStreamConfig),
    )

    def __call__(
        self, ctx: ExecutionContext, spec: StreamSpec[Any]
    ) -> StreamQueryPort[Any]:
        return _stream_adapter(ctx, spec, self.config)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisStreamCommand(StreamCommandDepPort):
    """Build an append :class:`RedisStreamAdapter`, encryption-wrapped per ``spec``."""

    config: RedisStreamConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisStreamConfig),
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
        adapter = _stream_adapter(ctx, spec, self.config)
        cipher = (
            ctx.deps.provide(KeyringDepKey)
            if ctx.deps.exists(KeyringDepKey)
            else None
        )
        return encrypting_stream_command(
            adapter, spec, cipher=cipher, tenant_provider=ctx.inv_ctx.get_tenant
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisStreamGroup(StreamGroupQueryDepPort):
    """Build a :class:`RedisStreamGroupAdapter` (consumer-group reads/ack/claim/pending)."""

    config: RedisStreamGroupConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisStreamGroupConfig),
    )

    def __call__(
        self, ctx: ExecutionContext, spec: StreamSpec[Any]
    ) -> StreamGroupQueryPort[Any]:
        return RedisStreamGroupAdapter(
            client=ctx.deps.provide(RedisClientDepKey),
            codec=RedisStreamCodec(payload_codec=spec.codec),
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisStreamGroupAdmin(StreamGroupAdminDepPort):
    """Build a :class:`RedisStreamGroupAdminAdapter` (group provisioning only)."""

    config: RedisStreamGroupConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisStreamGroupConfig),
    )

    def __call__(
        self, ctx: ExecutionContext, spec: StreamSpec[Any]
    ) -> StreamGroupAdminPort:
        return RedisStreamGroupAdminAdapter(
            client=ctx.deps.provide(RedisClientDepKey),
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
