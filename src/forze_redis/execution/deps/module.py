"""Redis dependency module for the application kernel."""

from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.cache import CacheDepKey
from forze.application.contracts.counter import CounterDepKey
from forze.application.contracts.idempotency import IdempotencyDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import RedisClient
from .configs import RedisCacheConfig, RedisCounterConfig, RedisIdempotencyConfig
from .deps import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisIdempotency,
)
from .keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisDepsModule(DepsModule):
    """Dependency module that registers Redis clients and adapters."""

    client: RedisClient
    """Pre-constructed Redis client (pool not yet initialized)."""

    caches: Mapping[str | StrEnum, RedisCacheConfig] | None = None
    """Mapping from cache names to their Redis-specific configurations."""

    counters: Mapping[str | StrEnum, RedisCounterConfig] | None = None
    """Mapping from counter names to their Redis-specific configurations."""

    idempotency: Mapping[str | StrEnum, RedisIdempotencyConfig] | None = None
    """Mapping from idempotency names to their Redis-specific configurations."""

    #! read and write separately?

    # pubsub: dict[str, RedisPubSubConfig] = attrs.field(factory=dict)
    # """Mapping from pubsub names to their Redis-specific configurations."""

    # streams: dict[str, RedisStreamConfig] = attrs.field(factory=dict)
    # """Mapping from stream names to their Redis-specific configurations."""

    # stream_groups: dict[str, RedisStreamGroupConfig] = attrs.field(factory=dict)
    # """Mapping from stream group names to their Redis-specific configurations."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Redis-backed ports."""

        plain_deps = Deps.plain({RedisClientDepKey: self.client})

        cache_deps = Deps()
        counter_deps = Deps()
        idempotency_deps = Deps()

        if self.caches:
            cache_deps = cache_deps.merge(
                Deps.routed(
                    {
                        CacheDepKey: {
                            name: ConfigurableRedisCache(config=config)
                            for name, config in self.caches.items()
                        }
                    }
                )
            )

        if self.counters:
            counter_deps = counter_deps.merge(
                Deps.routed(
                    {
                        CounterDepKey: {
                            name: ConfigurableRedisCounter(config=config)
                            for name, config in self.counters.items()
                        }
                    }
                )
            )

        if self.idempotency:
            idempotency_deps = idempotency_deps.merge(
                Deps.routed(
                    {
                        IdempotencyDepKey: {
                            name: ConfigurableRedisIdempotency(config=config)
                            for name, config in self.idempotency.items()
                        }
                    }
                )
            )

        return plain_deps.merge(cache_deps, counter_deps, idempotency_deps)
