"""Redis execution wiring for the application kernel.

Provides :class:`RedisDepsModule` (dependency module registering client,
document cache, counter, idempotency, pubsub, and stream ports),
:data:`RedisClientDepKey`, and :func:`redis_lifecycle_step` for startup/shutdown
of the Redis client.
"""

from .deps import RedisClientDepKey, RedisDepsModule
from .lifecycle import redis_lifecycle_step

# ----------------------- #

__all__ = [
    "RedisDepsModule",
    "RedisClientDepKey",
    "redis_lifecycle_step",
]
