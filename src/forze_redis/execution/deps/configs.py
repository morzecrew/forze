from typing import Any, NotRequired, TypedDict, TypeGuard, final

# ----------------------- #


class _BaseRedisConfig(TypedDict):
    """Base configuration for a Redis resource."""

    namespace: str
    """Namespace for the keys."""

    tenant_aware: NotRequired[bool]
    """Whether the resource is tenant-aware."""


# ....................... #


@final
class RedisCacheConfig(_BaseRedisConfig):
    """Configuration for a Redis cache."""


# ....................... #


@final
class RedisCounterConfig(_BaseRedisConfig):
    """Configuration for a Redis counter."""


# ....................... #


@final
class RedisIdempotencyConfig(_BaseRedisConfig):
    """Configuration for a Redis idempotency."""


# ....................... #


def is_redis_idempotency_config(
    config: dict[str, Any],
) -> TypeGuard[RedisIdempotencyConfig]:
    return "namespace" in config


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
