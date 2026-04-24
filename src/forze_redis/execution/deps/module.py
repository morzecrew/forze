"""Redis dependency module for the application kernel."""

from collections.abc import Mapping as MappingABC
from enum import StrEnum
from typing import Any, Mapping, TypeGuard, final

import attrs

from forze.application.contracts.cache import CacheDepKey
from forze.application.contracts.counter import CounterDepKey
from forze.application.contracts.idempotency import IdempotencyDepKey
from forze.application.contracts.search import SearchResultSnapshotDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import RedisClient
from .configs import (
    RedisCacheConfig,
    RedisCounterConfig,
    RedisIdempotencyConfig,
    RedisSearchResultSnapshotConfig,
    RedisUniversalConfig,
)
from .deps import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisIdempotency,
    ConfigurableRedisSearchResultSnapshot,
)
from .keys import RedisClientDepKey

# ----------------------- #


def _is_idem_routed(config: Any) -> TypeGuard[Mapping[Any, RedisIdempotencyConfig]]:
    if not isinstance(config, MappingABC):
        return False

    k = list(config.keys())  # type: ignore

    return isinstance(config[k[0]], MappingABC)


def _is_idem_plain(config: Any) -> TypeGuard[RedisIdempotencyConfig]:
    if not isinstance(config, MappingABC):
        return False

    k = list(config.keys())  # type: ignore

    return not isinstance(config[k[0]], MappingABC)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers Redis clients and adapters."""

    client: RedisClient
    """Pre-constructed Redis client (pool not yet initialized)."""

    caches: Mapping[K, RedisCacheConfig | RedisUniversalConfig] | None = attrs.field(
        default=None
    )
    """Mapping from cache names to their Redis-specific configurations."""

    counters: Mapping[K, RedisCounterConfig | RedisUniversalConfig] | None = (
        attrs.field(default=None)
    )
    """Mapping from counter names to their Redis-specific configurations."""

    idempotency: (
        Mapping[K, RedisIdempotencyConfig | RedisUniversalConfig]
        | RedisIdempotencyConfig
        | RedisUniversalConfig
        | None
    ) = attrs.field(default=None)
    """Redis-specific configurations for idempotency."""

    search_snapshots: (
        Mapping[K, RedisSearchResultSnapshotConfig | RedisUniversalConfig] | None
    ) = attrs.field(default=None)
    """Mapping from search snapshot names to their Redis-specific configurations."""

    #! read and write separately?

    # pubsub: dict[str, RedisPubSubConfig] = attrs.field(factory=dict)
    # """Mapping from pubsub names to their Redis-specific configurations."""

    # streams: dict[str, RedisStreamConfig] = attrs.field(factory=dict)
    # """Mapping from stream names to their Redis-specific configurations."""

    # stream_groups: dict[str, RedisStreamGroupConfig] = attrs.field(factory=dict)
    # """Mapping from stream group names to their Redis-specific configurations."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        """Build a dependency container with Redis-backed ports."""

        plain_deps = Deps[K].plain({RedisClientDepKey: self.client})

        cache_deps = Deps[K]()
        counter_deps = Deps[K]()
        idempotency_deps = Deps[K]()
        search_snapshot_deps = Deps[K]()

        if self.caches:
            cache_deps = cache_deps.merge(
                Deps[K].routed(
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
                Deps[K].routed(
                    {
                        CounterDepKey: {
                            name: ConfigurableRedisCounter(config=config)
                            for name, config in self.counters.items()
                        }
                    }
                )
            )

        if self.idempotency:
            if _is_idem_routed(self.idempotency):
                idempotency_deps = idempotency_deps.merge(
                    Deps[K].routed(
                        {
                            IdempotencyDepKey: {
                                name: ConfigurableRedisIdempotency(config=config)
                                for name, config in self.idempotency.items()
                            }
                        }
                    )
                )

            elif _is_idem_plain(self.idempotency):
                idempotency_deps = idempotency_deps.merge(
                    Deps[K].plain(
                        {
                            IdempotencyDepKey: ConfigurableRedisIdempotency(
                                config=self.idempotency
                            )
                        }
                    )
                )

        if self.search_snapshots:
            search_snapshot_deps = search_snapshot_deps.merge(
                Deps[K].routed(
                    {
                        SearchResultSnapshotDepKey: {
                            name: ConfigurableRedisSearchResultSnapshot(config=config)
                            for name, config in self.search_snapshots.items()
                        }
                    }
                )
            )

        return plain_deps.merge(
            cache_deps, counter_deps, idempotency_deps, search_snapshot_deps
        )
