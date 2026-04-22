from typing import final

import attrs

from forze.application.contracts.cache import CacheDepPort, CacheSpec
from forze.application.contracts.counter import CounterDepPort, CounterSpec
from forze.application.contracts.idempotency import IdempotencyDepPort, IdempotencySpec
from forze.application.execution import ExecutionContext

from ...adapters import (
    RedisCacheAdapter,
    RedisCounterAdapter,
    RedisIdempotencyAdapter,
    RedisKeyCodec,
)
from .configs import (
    RedisCacheConfig,
    RedisCounterConfig,
    RedisIdempotencyConfig,
    RedisUniversalConfig,
)
from .keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisCache(CacheDepPort):
    """Configurable Redis cache adapter."""

    config: RedisCacheConfig | RedisUniversalConfig
    """Configuration for the cache."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CacheSpec,
    ) -> RedisCacheAdapter:
        client = ctx.dep(RedisClientDepKey)
        key_codec = RedisKeyCodec(namespace=str(self.config["namespace"]))

        return RedisCacheAdapter(
            client=client,
            key_codec=key_codec,
            ttl_pointer=spec.ttl_pointer,
            ttl_body=spec.ttl,
            ttl_kv=spec.ttl,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenant_id,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisCounter(CounterDepPort):
    """Configurable Redis counter adapter."""

    config: RedisCounterConfig | RedisUniversalConfig
    """Configuration for the counter."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: CounterSpec,
    ) -> RedisCounterAdapter:
        client = ctx.dep(RedisClientDepKey)
        key_codec = RedisKeyCodec(namespace=str(self.config["namespace"]))

        return RedisCounterAdapter(
            client=client,
            key_codec=key_codec,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenant_id,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisIdempotency(IdempotencyDepPort):
    """Configurable Redis idempotency adapter."""

    config: RedisIdempotencyConfig | RedisUniversalConfig
    """Configuration for the idempotency."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: IdempotencySpec,
    ) -> RedisIdempotencyAdapter:
        client = ctx.dep(RedisClientDepKey)
        key_codec = RedisKeyCodec(namespace=str(self.config["namespace"]))

        return RedisIdempotencyAdapter(
            client=client,
            key_codec=key_codec,
            ttl=spec.ttl,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenant_id,
        )
