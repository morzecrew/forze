from forze.application.contracts.deps import DepKey

from ..kernel.platform import RedisClient

# ----------------------- #

RedisClientDepKey: DepKey[RedisClient] = DepKey("redis_client")
"""Key used to register the :class:`RedisClient` implementation."""
