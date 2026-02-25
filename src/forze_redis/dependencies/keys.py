from forze.application.kernel.dependencies import DependencyKey

from ..kernel.platform import RedisClient

# ----------------------- #

RedisClientDependencyKey: DependencyKey[RedisClient] = DependencyKey("redis_client")
"""Key used to register the :class:`RedisClient` implementation."""
