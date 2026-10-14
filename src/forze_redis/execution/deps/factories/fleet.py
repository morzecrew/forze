"""Redis fleet membership dep factory."""

from typing import final

import attrs

from forze.application.contracts.fleet import FleetSpec
from forze.application.execution import ExecutionContext

from ....adapters import RedisFleetAdapter
from ..configs import RedisFleetConfig, RedisUniversalConfig
from ..keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisFleet:
    """Build :class:`RedisFleetAdapter` from execution context and fleet spec."""

    config: RedisFleetConfig | RedisUniversalConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisUniversalConfig),
    )
    """Configuration (namespace, optional tenant awareness)."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: FleetSpec) -> RedisFleetAdapter:
        return RedisFleetAdapter(
            client=ctx.deps.provide(RedisClientDepKey),
            namespace=self.config.namespace,
            spec=spec,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
