"""Redis dependency module for the application kernel."""

from collections.abc import Mapping as MappingABC
from enum import StrEnum
from typing import Any, Mapping, TypeGuard, cast, final

import attrs

from forze.application.contracts.cache import CacheDepKey
from forze.application.contracts.counter import CounterDepKey
from forze.application.contracts.dlock import (
    DistributedLockCommandDepKey,
    DistributedLockQueryDepKey,
)
from forze.application.contracts.idempotency import IdempotencyDepKey
from forze.application.contracts.search import SearchResultSnapshotDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import RedisClientPort
from .configs import (
    RedisCacheConfig,
    RedisCounterConfig,
    RedisDistributedLockConfig,
    RedisIdempotencyConfig,
    RedisSearchResultSnapshotConfig,
    RedisUniversalConfig,
)
from .deps import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisDistributedLock,
    ConfigurableRedisIdempotency,
    ConfigurableRedisSearchResultSnapshot,
)
from .keys import RedisBlockingClientDepKey, RedisClientDepKey

# ----------------------- #


def _idem_mapping_keys(config: Any) -> list[Any]:
    if not isinstance(config, MappingABC):
        return []

    return list(config.keys())  # type: ignore[reportUnknownArgumentType]


def _is_idem_routed(config: Any) -> TypeGuard[Mapping[Any, RedisIdempotencyConfig]]:
    if not isinstance(config, MappingABC):
        return False

    routes = cast(Mapping[Any, Any], config)  # type: ignore[redundant-cast]

    if len(routes) < 1:
        return False

    for v in routes.values():
        if not isinstance(v, MappingABC) or "namespace" not in v:
            return False

    return True


def _is_idem_plain(config: Any) -> TypeGuard[RedisIdempotencyConfig]:
    keys = _idem_mapping_keys(config)

    if not keys:
        return False

    return not isinstance(config[keys[0]], MappingABC)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers Redis clients and adapters."""

    client: RedisClientPort
    """Pre-constructed Redis client (single-DSN or routed, not initialized until lifecycle)."""

    blocking_client: RedisClientPort | None = None
    """Optional second client registered under :data:`RedisBlockingClientDepKey`."""

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

    dlocks: Mapping[K, RedisDistributedLockConfig | RedisUniversalConfig] | None = (
        attrs.field(default=None)
    )
    """Mapping from distributed lock spec names to their Redis-specific configurations."""

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

        plain: dict[Any, Any] = {RedisClientDepKey: self.client}

        if self.blocking_client is not None:
            plain[RedisBlockingClientDepKey] = self.blocking_client

        plain_deps = Deps[K].plain(plain)

        cache_deps = Deps[K]()
        counter_deps = Deps[K]()
        idempotency_deps = Deps[K]()
        search_snapshot_deps = Deps[K]()
        dlock_deps = Deps[K]()

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

        if self.dlocks:
            dlock_factories = {
                name: ConfigurableRedisDistributedLock(config=config)
                for name, config in self.dlocks.items()
            }
            dlock_deps = dlock_deps.merge(
                Deps[K].routed(
                    {
                        DistributedLockQueryDepKey: dlock_factories,
                        DistributedLockCommandDepKey: dlock_factories,
                    }
                )
            )

        return plain_deps.merge(
            cache_deps,
            counter_deps,
            idempotency_deps,
            search_snapshot_deps,
            dlock_deps,
        )
