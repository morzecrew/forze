from enum import StrEnum
from typing import NotRequired, TypedDict, final

# ----------------------- #


class RedisUniversalConfig(TypedDict):
    """Base configuration for a Redis resource."""

    namespace: str | StrEnum
    """Namespace for the keys."""

    tenant_aware: NotRequired[bool]
    """Whether the resource is tenant-aware."""


# ....................... #


@final
class RedisCacheConfig(RedisUniversalConfig):
    """Configuration for a Redis cache."""


# ....................... #


@final
class RedisCounterConfig(RedisUniversalConfig):
    """Configuration for a Redis counter."""


# ....................... #


@final
class RedisIdempotencyConfig(RedisUniversalConfig):
    """Configuration for a Redis idempotency."""


# ....................... #
#! very questionable to have namespace inside pubsub and streams

# @final
# class RedisPubSubConfig(_BaseRedisConfig):
#     """Configuration for a Redis pub/sub."""


# # ....................... #


# @final
# class RedisStreamConfig(_BaseRedisConfig):
#     """Configuration for a Redis stream."""


# # ....................... #


# @final
# class RedisStreamGroupConfig(_BaseRedisConfig):
#     """Configuration for a Redis stream group."""
