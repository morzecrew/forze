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
from .keys import RedisClientDepKey
from .module import RedisDepsModule

# ----------------------- #

__all__ = [
    "RedisDepsModule",
    "RedisClientDepKey",
    "RedisCacheConfig",
    "RedisCounterConfig",
    "RedisDistributedLockConfig",
    "RedisIdempotencyConfig",
    "RedisSearchResultSnapshotConfig",
    "RedisUniversalConfig",
    "ConfigurableRedisDistributedLock",
    "ConfigurableRedisSearchResultSnapshot",
]
