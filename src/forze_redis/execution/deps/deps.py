from typing import final

import attrs

from forze.application.contracts.cache import CacheDepPort, CacheSpec
from forze.application.contracts.counter import CounterDepPort, CounterSpec
from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.idempotency import IdempotencyDepPort, IdempotencySpec
from forze.application.contracts.search import (
    SearchResultSnapshotPort,
    SearchResultSnapshotSpec,
)
from forze.application.execution import ExecutionContext

from ...adapters import (
    RedisCacheAdapter,
    RedisCounterAdapter,
    RedisDistributedLockAdapter,
    RedisIdempotencyAdapter,
    RedisKeyCodec,
    RedisSearchResultSnapshotAdapter,
)
from .configs import (
    RedisCacheConfig,
    RedisCounterConfig,
    RedisDistributedLockConfig,
    RedisIdempotencyConfig,
    RedisSearchResultSnapshotConfig,
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
            tenant_provider=ctx.get_tenancy_identity,
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
            tenant_provider=ctx.get_tenancy_identity,
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
            tenant_provider=ctx.get_tenancy_identity,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisSearchResultSnapshot:
    """Build :class:`RedisSearchResultSnapshotAdapter` from execution context and store spec."""

    config: RedisSearchResultSnapshotConfig | RedisUniversalConfig
    """Configuration (namespace, optional tenant)."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: SearchResultSnapshotSpec,
    ) -> SearchResultSnapshotPort:
        client = ctx.dep(RedisClientDepKey)
        key_codec = RedisKeyCodec(namespace=str(self.config["namespace"]))

        return RedisSearchResultSnapshotAdapter(
            client=client,
            key_codec=key_codec,
            default_ttl=spec.ttl,
            default_max_ids=spec.max_ids,
            default_chunk_size=spec.chunk_size,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenancy_identity,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRedisDistributedLock:
    """Build :class:`RedisDistributedLockAdapter` from execution context and lock spec."""

    config: RedisDistributedLockConfig | RedisUniversalConfig
    """Configuration (namespace, optional tenant awareness)."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DistributedLockSpec,
    ) -> RedisDistributedLockAdapter:
        client = ctx.dep(RedisClientDepKey)
        key_codec = RedisKeyCodec(namespace=str(self.config["namespace"]))

        return RedisDistributedLockAdapter(
            client=client,
            key_codec=key_codec,
            spec=spec,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenancy_identity,
        )
