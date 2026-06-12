"""Redis dependency integration configs (frozen attrs)."""

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisUniversalConfig(TenantAwareIntegrationConfig):
    """Base configuration for a Redis resource."""

    namespace: NamedResourceSpec = attrs.field(converter=coerce_named_resource_spec)
    """Namespace for the keys."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisCacheConfig(RedisUniversalConfig):
    """Configuration for a Redis cache."""

    invalidation_push: bool = False
    """Opt-in client-side-caching invalidation push (Redis 6+ ``CLIENT
    TRACKING``): the document L1 drops entries when any replica writes them,
    demoting the L1 TTL to a backstop. Requires a static namespace and a
    non-routed client; unsupported setups silently stay TTL-only."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisCounterConfig(RedisUniversalConfig):
    """Configuration for a Redis counter."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisIdempotencyConfig(RedisUniversalConfig):
    """Configuration for a Redis idempotency store."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisSearchResultSnapshotConfig(RedisUniversalConfig):
    """Configuration for the search result snapshot store."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisDistributedLockConfig(RedisUniversalConfig):
    """Configuration for Redis-backed distributed locks."""
