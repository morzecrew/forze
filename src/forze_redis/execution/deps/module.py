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
from forze.application.contracts.tenancy import warn_integration_routes
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import (
    merge_deps,
    routed_from_mapping,
    routed_shared_factories,
)
from forze.base.primitives import StrKey

from ...kernel._logger import logger
from ...kernel.client import RedisClientPort
from ._warnings import (
    REDIS_CACHE_WARNING,
    REDIS_COUNTER_WARNING,
    REDIS_DLOCK_WARNING,
    REDIS_IDEMPOTENCY_WARNING,
    REDIS_SEARCH_SNAPSHOT_WARNING,
)
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


def _is_idem_plain(config: Any) -> TypeGuard[RedisIdempotencyConfig | RedisUniversalConfig]:
    if isinstance(config, RedisIdempotencyConfig):
        return True

    return type(config) is RedisUniversalConfig


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
        warn_integration_routes(
            integration="Redis",
            routes=self.caches,
            warning=REDIS_CACHE_WARNING,
            log_warning=logger.warning,
        )
        warn_integration_routes(
            integration="Redis",
            routes=self.counters,
            warning=REDIS_COUNTER_WARNING,
            log_warning=logger.warning,
        )

        if self.idempotency:
            if _is_idem_routed(self.idempotency):
                warn_integration_routes(
                    integration="Redis",
                    routes=self.idempotency,
                    warning=REDIS_IDEMPOTENCY_WARNING,
                    log_warning=logger.warning,
                )

            elif _is_idem_plain(self.idempotency):
                warn_integration_routes(
                    integration="Redis",
                    routes={"idempotency": self.idempotency},
                    warning=REDIS_IDEMPOTENCY_WARNING,
                    log_warning=logger.warning,
                )

        warn_integration_routes(
            integration="Redis",
            routes=self.search_snapshots,
            warning=REDIS_SEARCH_SNAPSHOT_WARNING,
            log_warning=logger.warning,
        )
        warn_integration_routes(
            integration="Redis",
            routes=self.dlocks,
            warning=REDIS_DLOCK_WARNING,
            log_warning=logger.warning,
        )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Redis-backed ports."""

        plain: dict[Any, Any] = {RedisClientDepKey: self.client}

        if self.blocking_client is not None:
            plain[RedisBlockingClientDepKey] = self.blocking_client

        idempotency_deps = Deps()

        if self.idempotency:
            if _is_idem_routed(self.idempotency):
                idempotency_deps = routed_from_mapping(
                    self.idempotency,
                    bindings=[(IdempotencyDepKey, ConfigurableRedisIdempotency)],
                )

            elif _is_idem_plain(self.idempotency):
                idempotency_deps = Deps.plain(
                    {
                        IdempotencyDepKey: ConfigurableRedisIdempotency(
                            config=self.idempotency
                        )
                    }
                )

        return merge_deps(
            routed_from_mapping(
                self.caches,
                bindings=[(CacheDepKey, ConfigurableRedisCache)],
            ),
            routed_from_mapping(
                self.counters,
                bindings=[(CounterDepKey, ConfigurableRedisCounter)],
            ),
            idempotency_deps,
            routed_from_mapping(
                self.search_snapshots,
                bindings=[(SearchResultSnapshotDepKey, ConfigurableRedisSearchResultSnapshot)],
            ),
            routed_shared_factories(
                self.dlocks,
                dep_keys=[
                    DistributedLockQueryDepKey,
                    DistributedLockCommandDepKey,
                ],
                factory=ConfigurableRedisDistributedLock,
            ),
            plain=plain,
        )
