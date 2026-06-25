"""Redis dependency factories."""

from .cache import ConfigurableRedisCache
from .counter import ConfigurableRedisCounter
from .dlock import ConfigurableRedisDistributedLock
from .idempotency import ConfigurableRedisIdempotency
from .pubsub import ConfigurableRedisPubSubCommand, ConfigurableRedisPubSubQuery
from .snapshot import ConfigurableRedisSearchResultSnapshot
from .stream import (
    ConfigurableRedisStreamCommand,
    ConfigurableRedisStreamGroup,
    ConfigurableRedisStreamGroupAdmin,
    ConfigurableRedisStreamQuery,
)

# ----------------------- #

__all__ = [
    "ConfigurableRedisCache",
    "ConfigurableRedisCounter",
    "ConfigurableRedisDistributedLock",
    "ConfigurableRedisIdempotency",
    "ConfigurableRedisPubSubCommand",
    "ConfigurableRedisPubSubQuery",
    "ConfigurableRedisSearchResultSnapshot",
    "ConfigurableRedisStreamCommand",
    "ConfigurableRedisStreamGroup",
    "ConfigurableRedisStreamGroupAdmin",
    "ConfigurableRedisStreamQuery",
]
