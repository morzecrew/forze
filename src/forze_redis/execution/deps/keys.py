"""Dependency keys for Redis-related services."""

from forze.application.contracts.base import DepKey

from ...kernel.platform import RedisClient

# ----------------------- #

RedisClientDepKey: DepKey[RedisClient] = DepKey("redis_client")
"""Key used to register the :class:`RedisClient` in the deps container."""
