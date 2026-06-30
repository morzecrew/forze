"""Redis dependency module for the application kernel."""

from collections.abc import Mapping
from typing import Any, TypeGuard, cast, final

import attrs

from forze.application.contracts.cache import CacheDepKey
from forze.application.contracts.counter import CounterDepKey
from forze.application.contracts.deps import (
    Deps,
    DepsModule,
    merge_deps,
    routed_from_mapping,
    routed_shared_factories,
)
from forze.application.contracts.dlock import (
    DistributedLockCommandDepKey,
    DistributedLockQueryDepKey,
)
from forze.application.contracts.idempotency import IdempotencyDepKey
from forze.application.contracts.pubsub import (
    PubSubCommandDepKey,
    PubSubQueryDepKey,
)
from forze.application.contracts.search import SearchResultSnapshotDepKey
from forze.application.contracts.stream import (
    StreamCommandDepKey,
    StreamGroupAdminDepKey,
    StreamGroupQueryDepKey,
    StreamQueryDepKey,
)
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
    warn_integration_routes,
)
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping

from ...kernel._logger import logger
from ...kernel.client import RedisClientPort, RoutedRedisClient
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
    RedisPubSubConfig,
    RedisSearchResultSnapshotConfig,
    RedisStreamConfig,
    RedisStreamGroupConfig,
    RedisUniversalConfig,
)
from .factories import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisDistributedLock,
    ConfigurableRedisIdempotency,
    ConfigurableRedisPubSubCommand,
    ConfigurableRedisPubSubQuery,
    ConfigurableRedisSearchResultSnapshot,
    ConfigurableRedisStreamCommand,
    ConfigurableRedisStreamGroup,
    ConfigurableRedisStreamGroupAdmin,
    ConfigurableRedisStreamQuery,
)
from .keys import RedisBlockingClientDepKey, RedisClientDepKey

# ----------------------- #


def _is_idem_route_value(value: Any) -> bool:
    return isinstance(value, (RedisIdempotencyConfig, RedisUniversalConfig))


def _is_idem_routed(
    config: Any,
) -> TypeGuard[Mapping[Any, RedisIdempotencyConfig | RedisUniversalConfig]]:
    if not isinstance(config, Mapping):
        return False

    routes = cast(Mapping[Any, Any], config)  # type: ignore[redundant-cast]

    if len(routes) < 1:
        return False

    return all(_is_idem_route_value(v) for v in routes.values())


def _is_idem_plain(
    config: Any,
) -> TypeGuard[RedisIdempotencyConfig | RedisUniversalConfig]:
    if isinstance(config, RedisIdempotencyConfig):
        return True

    return type(config) is RedisUniversalConfig


# ....................... #


def _redis_idem_converter(
    value: (
        Mapping[StrKey, RedisIdempotencyConfig]
        | Mapping[StrKey, RedisUniversalConfig]
        | RedisIdempotencyConfig
        | RedisUniversalConfig
        | None
    ),
) -> (
    StrKeyMapping[RedisIdempotencyConfig]
    | StrKeyMapping[RedisUniversalConfig]
    | RedisIdempotencyConfig
    | RedisUniversalConfig
    | None
):
    if value is None:
        return None

    if isinstance(value, RedisIdempotencyConfig | RedisUniversalConfig):
        return value

    return MappingConverter.to_str_key_frozen(value)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisDepsModule(DepsModule):
    """Dependency module that registers Redis clients and adapters."""

    client: RedisClientPort
    """Pre-constructed Redis client (single-DSN or routed, not initialized until lifecycle)."""

    blocking_client: RedisClientPort | None = None
    """Optional second client registered under :data:`RedisBlockingClientDepKey`."""

    caches: (
        StrKeyMapping[RedisCacheConfig] | StrKeyMapping[RedisUniversalConfig] | None
    ) = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from cache names to their Redis-specific configurations."""

    counters: (
        StrKeyMapping[RedisCounterConfig] | StrKeyMapping[RedisUniversalConfig] | None
    ) = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )

    """Mapping from counter names to their Redis-specific configurations."""

    idempotency: (
        StrKeyMapping[RedisIdempotencyConfig]
        | StrKeyMapping[RedisUniversalConfig]
        | RedisIdempotencyConfig
        | RedisUniversalConfig
        | None
    ) = attrs.field(
        default=None,
        converter=_redis_idem_converter,
    )
    """Redis-specific configurations for idempotency."""

    search_snapshots: (
        StrKeyMapping[RedisSearchResultSnapshotConfig]
        | StrKeyMapping[RedisUniversalConfig]
        | None
    ) = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from search snapshot names to their Redis-specific configurations."""

    dlocks: (
        StrKeyMapping[RedisDistributedLockConfig]
        | StrKeyMapping[RedisUniversalConfig]
        | None
    ) = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from distributed lock spec names to their Redis-specific configurations."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Redis spans: ``tagged`` (per-tenant key prefix via ``tenant_aware``), ``namespace`` (a
    per-tenant ``namespace`` resolver), ``dedicated`` (a routed per-tenant client).
    """

    streams: StrKeyMapping[RedisStreamConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from stream route names to their Redis stream configurations.

    Registered under both ``StreamQueryDepKey`` (raw reads) and ``StreamCommandDepKey``
    (append, encryption-wrapped per ``StreamSpec.encryption``)."""

    stream_groups: StrKeyMapping[RedisStreamGroupConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from stream consumer-group route names to their configurations.

    Registered under ``StreamGroupQueryDepKey`` (read/ack/claim/pending) and
    ``StreamGroupAdminDepKey`` (group provisioning)."""

    pubsub: StrKeyMapping[RedisPubSubConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from pub-sub route names to their Redis pub-sub configurations.

    Registered under ``PubSubQueryDepKey`` (subscribe) and ``PubSubCommandDepKey``
    (publish, encryption-wrapped). Pub-sub is at-most-once past the broker."""

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

        idempotency_routes: Mapping[Any, Any] | None
        if self.idempotency is None:
            idempotency_routes = None
        elif _is_idem_routed(self.idempotency):
            idempotency_routes = self.idempotency
        else:
            idempotency_routes = {"idempotency": self.idempotency}

        validate_module_tenancy(
            integration="Redis",
            client_is_routed=isinstance(self.client, RoutedRedisClient),
            groups=[
                TenancyRouteGroup(
                    kind=kind,
                    configs=configs,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.namespace,
                )
                for kind, configs in (
                    ("cache", self.caches),
                    ("counter", self.counters),
                    ("idempotency", idempotency_routes),
                    ("search_snapshot", self.search_snapshots),
                    ("dlock", self.dlocks),
                )
            ]
            + [
                # Streams / pub-sub have no per-route key namespace — tenant isolation is
                # the ``tenant:{id}:`` key prefix (tagged tier) only, never namespace tier.
                TenancyRouteGroup(
                    kind=kind,
                    configs=configs,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda _cfg: None,
                )
                for kind, configs in (
                    ("stream", self.streams),
                    ("stream_group", self.stream_groups),
                    ("pubsub", self.pubsub),
                )
            ],
            required_isolation=self.required_tenant_isolation,
            max_supported_isolation="dedicated",
            validation_failed_code="redis_tenancy_validation_failed",
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
                bindings=[
                    (SearchResultSnapshotDepKey, ConfigurableRedisSearchResultSnapshot)
                ],
            ),
            routed_shared_factories(
                self.dlocks,
                dep_keys=[
                    DistributedLockQueryDepKey,
                    DistributedLockCommandDepKey,
                ],
                factory=ConfigurableRedisDistributedLock,
            ),
            routed_from_mapping(
                self.streams,
                bindings=[
                    (StreamQueryDepKey, ConfigurableRedisStreamQuery),
                    (StreamCommandDepKey, ConfigurableRedisStreamCommand),
                ],
            ),
            routed_from_mapping(
                self.stream_groups,
                bindings=[
                    (StreamGroupQueryDepKey, ConfigurableRedisStreamGroup),
                    (StreamGroupAdminDepKey, ConfigurableRedisStreamGroupAdmin),
                ],
            ),
            routed_from_mapping(
                self.pubsub,
                bindings=[
                    (PubSubQueryDepKey, ConfigurableRedisPubSubQuery),
                    (PubSubCommandDepKey, ConfigurableRedisPubSubCommand),
                ],
            ),
            plain=plain,
        )
