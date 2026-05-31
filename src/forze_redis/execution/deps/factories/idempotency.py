"""Redis idempotency dep factory."""

from typing import final

import attrs

from forze.application.contracts.idempotency import IdempotencyDepPort, IdempotencySpec
from forze.application.execution import ExecutionContext

from ....adapters import RedisIdempotencyAdapter
from ..configs import RedisIdempotencyConfig, RedisUniversalConfig
from ..keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisIdempotency(IdempotencyDepPort):
    """Configurable Redis idempotency adapter."""

    config: RedisIdempotencyConfig | RedisUniversalConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisUniversalConfig),
    )
    """Configuration for the idempotency."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: IdempotencySpec,
    ) -> RedisIdempotencyAdapter:
        client = ctx.deps.provide(RedisClientDepKey)

        return RedisIdempotencyAdapter(
            client=client,
            namespace=self.config.namespace,
            ttl=spec.ttl,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
