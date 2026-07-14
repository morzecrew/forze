"""Redis dependency keys, module, and configurations."""

from .configs import (
    RedisCacheConfig,
    RedisCounterConfig,
    RedisDistributedLockConfig,
    RedisIdempotencyConfig,
    RedisPubSubConfig,
    RedisSearchResultSnapshotConfig,
    RedisStreamConfig,
    RedisStreamGroupConfig,
    RedisUniversalConfig,
)
from .factories import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisCounterAdmin,
    ConfigurableRedisDistributedLock,
    ConfigurableRedisIdempotency,
    ConfigurableRedisPubSubCommand,
    ConfigurableRedisPubSubQuery,
    ConfigurableRedisSearchResultSnapshot,
    ConfigurableRedisStreamCommand,
    ConfigurableRedisStreamGroup,
    ConfigurableRedisStreamGroupAdmin,
    ConfigurableRedisStreamQuery,
)
from .keys import RedisBlockingClientDepKey, RedisClientDepKey
from .module import RedisDepsModule

# ----------------------- #

__all__ = [
    "RedisDepsModule",
    "RedisClientDepKey",
    "RedisBlockingClientDepKey",
    "RedisCacheConfig",
    "RedisCounterConfig",
    "RedisDistributedLockConfig",
    "RedisIdempotencyConfig",
    "RedisPubSubConfig",
    "RedisSearchResultSnapshotConfig",
    "RedisStreamConfig",
    "RedisStreamGroupConfig",
    "RedisUniversalConfig",
    "ConfigurableRedisCache",
    "ConfigurableRedisCounter",
    "ConfigurableRedisCounterAdmin",
    "ConfigurableRedisDistributedLock",
    "ConfigurableRedisIdempotency",
    "ConfigurableRedisPubSubCommand",
    "ConfigurableRedisPubSubQuery",
    "ConfigurableRedisSearchResultSnapshot",
    "ConfigurableRedisStreamCommand",
    "ConfigurableRedisStreamGroup",
    "ConfigurableRedisStreamGroupAdmin",
    "ConfigurableRedisStreamQuery",
]
