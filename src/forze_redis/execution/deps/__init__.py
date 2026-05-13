"""Redis dependency keys, module, and configurations."""

from .configs import (
    RedisCacheConfig,
    RedisCounterConfig,
    RedisDistributedLockConfig,
    RedisIdempotencyConfig,
    RedisSearchResultSnapshotConfig,
    RedisUniversalConfig,
)
from .deps import ConfigurableRedisDistributedLock, ConfigurableRedisSearchResultSnapshot
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
    "ConfigurableRedisDistributedLock",
    "ConfigurableRedisSearchResultSnapshot",
]
