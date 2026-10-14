"""Redis dependency factories."""

from .cache import ConfigurableRedisCache
from .counter import ConfigurableRedisCounter, ConfigurableRedisCounterAdmin
from .dlock import ConfigurableRedisDistributedLock
from .fleet import ConfigurableRedisFleet
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
    "ConfigurableRedisCounterAdmin",
    "ConfigurableRedisDistributedLock",
    "ConfigurableRedisFleet",
    "ConfigurableRedisIdempotency",
    "ConfigurableRedisPubSubCommand",
    "ConfigurableRedisPubSubQuery",
    "ConfigurableRedisSearchResultSnapshot",
    "ConfigurableRedisStreamCommand",
    "ConfigurableRedisStreamGroup",
    "ConfigurableRedisStreamGroupAdmin",
    "ConfigurableRedisStreamQuery",
]
