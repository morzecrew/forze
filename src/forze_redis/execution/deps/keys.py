"""Dependency keys for Redis-related services."""

from forze.application.contracts.base import DepKey

from ...kernel.platform import RedisClientPort

# ----------------------- #

RedisClientDepKey: DepKey[RedisClientPort] = DepKey("redis_client")
"""Key used to register a Redis client (single-DSN or routed) in the deps container."""
