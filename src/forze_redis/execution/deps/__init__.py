"""Redis dependency keys, module, and configurations."""

from .configs import (
    RedisCacheConfig,
    RedisCounterConfig,
    RedisDistributedLockConfig,
    RedisIdempotencyConfig,
    RedisSearchResultSnapshotConfig,
    RedisUniversalConfig,
)
from .factories import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisDistributedLock,
    ConfigurableRedisIdempotency,
    ConfigurableRedisSearchResultSnapshot,
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
    "RedisSearchResultSnapshotConfig",
    "RedisUniversalConfig",
    "ConfigurableRedisCache",
    "ConfigurableRedisCounter",
    "ConfigurableRedisDistributedLock",
    "ConfigurableRedisIdempotency",
    "ConfigurableRedisSearchResultSnapshot",
]
