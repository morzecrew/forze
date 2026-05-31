"""Redis distributed lock dep factory."""

from typing import final

import attrs

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.execution import ExecutionContext

from ....adapters import RedisDistributedLockAdapter
from ..configs import RedisDistributedLockConfig, RedisUniversalConfig
from ..keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisDistributedLock:
    """Build :class:`RedisDistributedLockAdapter` from execution context and lock spec."""

    config: RedisDistributedLockConfig | RedisUniversalConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisUniversalConfig),
    )
    """Configuration (namespace, optional tenant awareness)."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DistributedLockSpec,
    ) -> RedisDistributedLockAdapter:
        client = ctx.deps.provide(RedisClientDepKey)

        return RedisDistributedLockAdapter(
            client=client,
            namespace=self.config.namespace,
            spec=spec,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
