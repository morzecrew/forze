"""Redis / Valkey integration for Forze."""

from ._compat import require_redis

require_redis()

# ....................... #

from .execution import RedisClientDepKey, RedisDepsModule, redis_lifecycle_step
from .kernel.platform import RedisClient, RedisConfig

# ----------------------- #

__all__ = [
    "RedisClient",
    "RedisConfig",
    "RedisClientDepKey",
    "RedisDepsModule",
    "redis_lifecycle_step",
]
