"""Redis cache dep factory."""

from typing import final

import attrs

from forze.application.contracts.cache import CacheDepPort, CacheSpec
from forze.application.execution import ExecutionContext

from ....adapters import RedisCacheAdapter
from ..configs import RedisCacheConfig, RedisUniversalConfig
from ..keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisCache(CacheDepPort):
    """Configurable Redis cache adapter."""

    config: RedisCacheConfig | RedisUniversalConfig = attrs.field(
        validator=attrs.validators.instance_of(RedisUniversalConfig),
    )
    """Configuration for the cache."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CacheSpec,
    ) -> RedisCacheAdapter:
        client = ctx.deps.provide(RedisClientDepKey)

        return RedisCacheAdapter(
            client=client,
            namespace=self.config.namespace,
            ttl_pointer=spec.ttl_pointer,
            ttl_body=spec.ttl,
            ttl_kv=spec.ttl,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
            invalidation_push=getattr(self.config, "invalidation_push", False),
        )
