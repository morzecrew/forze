"""Redis dependency module for the application kernel."""

from collections.abc import Mapping as MappingABC
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
from forze.application.contracts.tenancy import warn_dynamic_relation_with_tenant_aware
from forze.application.execution import Deps, DepsModule
from forze.base.primitives import StrKey

from ...kernel._logger import logger
from ...kernel.client import RedisClientPort
from .configs import (
    RedisCacheConfig,
    RedisCounterConfig,
    RedisDistributedLockConfig,
    RedisIdempotencyConfig,
    RedisSearchResultSnapshotConfig,
    RedisUniversalConfig,
)
from .factories import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisDistributedLock,
    ConfigurableRedisIdempotency,
    ConfigurableRedisSearchResultSnapshot,
)
from .keys import RedisBlockingClientDepKey, RedisClientDepKey

# ----------------------- #


def _is_idem_routed(config: Any) -> TypeGuard[Mapping[Any, RedisIdempotencyConfig]]:
    if not isinstance(config, MappingABC):
        return False

    routes = cast(Mapping[Any, Any], config)  # type: ignore[redundant-cast]

    if len(routes) < 1:
        return False

    return all(isinstance(v, RedisIdempotencyConfig) for v in routes.values())


def _is_idem_plain(config: Any) -> TypeGuard[RedisIdempotencyConfig]:
    return isinstance(config, RedisIdempotencyConfig)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisDepsModule(DepsModule):
    """Dependency module that registers Redis clients and adapters."""

    client: RedisClientPort
    """Pre-constructed Redis client (single-DSN or routed, not initialized until lifecycle)."""

    blocking_client: RedisClientPort | None = None
    """Optional second client registered under :data:`RedisBlockingClientDepKey`."""

    caches: Mapping[StrKey, RedisCacheConfig | RedisUniversalConfig] | None = (
        attrs.field(default=None)
    )
    """Mapping from cache names to their Redis-specific configurations."""

    counters: Mapping[StrKey, RedisCounterConfig | RedisUniversalConfig] | None = (
        attrs.field(default=None)
    )
    """Mapping from counter names to their Redis-specific configurations."""

    idempotency: (
        Mapping[StrKey, RedisIdempotencyConfig | RedisUniversalConfig]
        | RedisIdempotencyConfig
        | RedisUniversalConfig
        | None
    ) = attrs.field(default=None)
    """Redis-specific configurations for idempotency."""

    search_snapshots: (
        Mapping[StrKey, RedisSearchResultSnapshotConfig | RedisUniversalConfig] | None
    ) = attrs.field(default=None)
    """Mapping from search snapshot names to their Redis-specific configurations."""

    dlocks: (
        Mapping[StrKey, RedisDistributedLockConfig | RedisUniversalConfig] | None
    ) = attrs.field(default=None)
    """Mapping from distributed lock spec names to their Redis-specific configurations."""

    #! read and write separately?

    # pubsub: dict[str, RedisPubSubConfig] = attrs.field(factory=dict)
    # """Mapping from pubsub names to their Redis-specific configurations."""

    # streams: dict[str, RedisStreamConfig] = attrs.field(factory=dict)
    # """Mapping from stream names to their Redis-specific configurations."""

    # stream_groups: dict[str, RedisStreamGroupConfig] = attrs.field(factory=dict)
    # """Mapping from stream group names to their Redis-specific configurations."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        def _warn_route(
            route_name: str,
            *,
            kind: str,
            config: RedisUniversalConfig,
        ) -> None:
            warn_dynamic_relation_with_tenant_aware(
                integration="Redis",
                route_name=route_name,
                kind=kind,
                tenant_aware=config.tenant_aware,
                named_fields=[("namespace", config.namespace)],
                log_warning=logger.warning,
            )

        if self.caches:
            for name, cfg in self.caches.items():
                _warn_route(str(name), kind="cache", config=cfg)

        if self.counters:
            for name, cfg in self.counters.items():
                _warn_route(str(name), kind="counter", config=cfg)

        if self.idempotency:
            if _is_idem_routed(self.idempotency):
                for name, cfg in self.idempotency.items():
                    _warn_route(str(name), kind="idempotency", config=cfg)

            elif _is_idem_plain(self.idempotency):
                _warn_route("idempotency", kind="idempotency", config=self.idempotency)

        if self.search_snapshots:
            for name, cfg in self.search_snapshots.items():
                _warn_route(str(name), kind="search_snapshot", config=cfg)

        if self.dlocks:
            for name, cfg in self.dlocks.items():
                _warn_route(str(name), kind="dlock", config=cfg)

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Redis-backed ports."""

        plain: dict[Any, Any] = {RedisClientDepKey: self.client}

        if self.blocking_client is not None:
            plain[RedisBlockingClientDepKey] = self.blocking_client

        plain_deps = Deps.plain(plain)

        cache_deps = Deps()
        counter_deps = Deps()
        idempotency_deps = Deps()
        search_snapshot_deps = Deps()
        dlock_deps = Deps()

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
            if _is_idem_routed(self.idempotency):
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

            elif _is_idem_plain(self.idempotency):
                idempotency_deps = idempotency_deps.merge(
                    Deps.plain(
                        {
                            IdempotencyDepKey: ConfigurableRedisIdempotency(
                                config=self.idempotency
                            )
                        }
                    )
                )

        if self.search_snapshots:
            search_snapshot_deps = search_snapshot_deps.merge(
                Deps.routed(
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
                Deps.routed(
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
