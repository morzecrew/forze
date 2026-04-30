"""Redis / Valkey integration for Forze."""

from ._compat import require_redis

require_redis()

# ....................... #

from .execution import (
    RedisCacheConfig,
    RedisClientDepKey,
    RedisCounterConfig,
    RedisDepsModule,
    RedisIdempotencyConfig,
    RedisUniversalConfig,
    redis_lifecycle_step,
    routed_redis_lifecycle_step,
)
from .kernel.platform import RedisClient, RedisClientPort, RedisConfig, RoutedRedisClient

# ----------------------- #

__all__ = [
    "RedisClient",
    "RedisClientPort",
    "RedisConfig",
    "RoutedRedisClient",
    "RedisClientDepKey",
    "RedisDepsModule",
    "redis_lifecycle_step",
    "routed_redis_lifecycle_step",
    "RedisCacheConfig",
    "RedisCounterConfig",
    "RedisIdempotencyConfig",
    "RedisUniversalConfig",
]
