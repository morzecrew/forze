"""Redis / Valkey integration for Forze."""

from ._compat import require_redis

require_redis()

# ....................... #

from .execution import (
    RedisBlockingClientDepKey,
    RedisCacheConfig,
    RedisClientDepKey,
    RedisCounterConfig,
    RedisDepsModule,
    RedisDistributedLockConfig,
    RedisIdempotencyConfig,
    RedisUniversalConfig,
    redis_lifecycle_step,
    routed_redis_lifecycle_step,
)
from .kernel.client import RedisClient, RedisClientPort, RedisConfig, RoutedRedisClient
from .kernel.relation import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    resolve_redis_namespace,
)

# ----------------------- #

__all__ = [
    "RedisClient",
    "RedisClientPort",
    "RedisConfig",
    "RoutedRedisClient",
    "RedisClientDepKey",
    "RedisBlockingClientDepKey",
    "RedisDepsModule",
    "redis_lifecycle_step",
    "routed_redis_lifecycle_step",
    "RedisCacheConfig",
    "RedisCounterConfig",
    "RedisDistributedLockConfig",
    "RedisIdempotencyConfig",
    "RedisUniversalConfig",
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "resolve_redis_namespace",
]
