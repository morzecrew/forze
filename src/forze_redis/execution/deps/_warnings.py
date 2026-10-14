"""Private tenancy warning descriptors for Redis deps module."""

from forze.application.contracts.tenancy import namespace_route_warning

from .configs import RedisUniversalConfig

# ----------------------- #

REDIS_CACHE_WARNING = namespace_route_warning(RedisUniversalConfig, kind="cache")
REDIS_COUNTER_WARNING = namespace_route_warning(RedisUniversalConfig, kind="counter")
REDIS_IDEMPOTENCY_WARNING = namespace_route_warning(RedisUniversalConfig, kind="idempotency")
REDIS_SEARCH_SNAPSHOT_WARNING = namespace_route_warning(
    RedisUniversalConfig, kind="search_snapshot"
)
REDIS_DLOCK_WARNING = namespace_route_warning(RedisUniversalConfig, kind="dlock")
