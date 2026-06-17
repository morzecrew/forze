"""Redis / Valkey integration for Forze."""

from ._compat import require_redis

require_redis()

# ....................... #

from .execution import (
    RedisBlockingClientDepKey,
    RedisCacheConfig,
    RedisClientDepKey,
    RedisCounterConfig,
    RedisDepsModule,
    RedisDistributedLockConfig,
    RedisIdempotencyConfig,
    RedisUniversalConfig,
    redis_lifecycle_step,
    routed_redis_lifecycle_step,
)
from .adapters.circuit_breaker import (
    RedisCircuitBreakerStore,
    redis_circuit_breaker_store,
)
from .adapters.latency_digest import (
    RedisLatencyDigestStore,
    redis_latency_digest_store,
)
from .adapters.rate_limit import (
    RedisRateLimitStore,
    redis_rate_limit_store,
)
from .kernel.client import RedisClient, RedisClientPort, RedisConfig, RoutedRedisClient
from .kernel.relation import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    resolve_redis_namespace,
)

# ----------------------- #

__all__ = [
    "RedisClient",
    "RedisClientPort",
    "RedisConfig",
    "RoutedRedisClient",
    "RedisClientDepKey",
    "RedisBlockingClientDepKey",
    "RedisDepsModule",
    "redis_lifecycle_step",
    "routed_redis_lifecycle_step",
    "RedisCacheConfig",
    "RedisCounterConfig",
    "RedisDistributedLockConfig",
    "RedisIdempotencyConfig",
    "RedisUniversalConfig",
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "resolve_redis_namespace",
    "RedisCircuitBreakerStore",
    "redis_circuit_breaker_store",
    "RedisRateLimitStore",
    "redis_rate_limit_store",
    "RedisLatencyDigestStore",
    "redis_latency_digest_store",
]
