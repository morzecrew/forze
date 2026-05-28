"""Redis dependency integration configs (frozen attrs)."""

from enum import StrEnum

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisUniversalConfig(TenantAwareIntegrationConfig):
    """Base configuration for a Redis resource."""

    namespace: str | StrEnum
    """Namespace for the keys."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisCacheConfig(RedisUniversalConfig):
    """Configuration for a Redis cache."""


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
