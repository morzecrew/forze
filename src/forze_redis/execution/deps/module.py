"""Redis dependency module for the application kernel."""

from collections.abc import Mapping
from typing import Any, TypeGuard, cast, final

import attrs

from forze.application.contracts.authn import RevocationStatesDepKey
from forze.application.contracts.cache import CacheDepKey
from forze.application.contracts.counter import CounterAdminDepKey, CounterDepKey
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
from forze.application.contracts.fleet import FleetDepKey, refuse_tenant_scoped_route
from forze.application.contracts.idempotency import IdempotencyDepKey
from forze.application.contracts.pubsub import (
    PubSubCommandDepKey,
    PubSubQueryDepKey,
)
from forze.application.contracts.resolution import is_static_named_resource
from forze.application.contracts.search import SearchResultSnapshotDepKey
from forze.application.contracts.stream import (
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    StreamCommandDepKey,
    StreamQueryDepKey,
)
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
    warn_integration_routes,
)
from forze.base.exceptions import exc
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
    RedisFleetConfig,
    RedisIdempotencyConfig,
    RedisPubSubConfig,
    RedisRevocationStatesConfig,
    RedisSearchResultSnapshotConfig,
    RedisStreamConfig,
    RedisStreamGroupConfig,
    RedisUniversalConfig,
)
from .factories import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisCounterAdmin,
    ConfigurableRedisDistributedLock,
    ConfigurableRedisFleet,
    ConfigurableRedisIdempotency,
    ConfigurableRedisPubSubCommand,
    ConfigurableRedisPubSubQuery,
    ConfigurableRedisRevocationStates,
    ConfigurableRedisSearchResultSnapshot,
    ConfigurableRedisStreamCommand,
    ConfigurableRedisStreamGroup,
    ConfigurableRedisStreamGroupAdmin,
    ConfigurableRedisStreamQuery,
)
from .keys import RedisBlockingClientDepKey, RedisClientDepKey

# ----------------------- #


def _never(_config: Any) -> bool:
    return False


def _always(_config: Any) -> bool:
    return True


def _marked_tenant_global(config: Any) -> bool:
    """Whether a lock or pub/sub route is marked the deployment's, outside the floor."""

    return isinstance(config, (RedisDistributedLockConfig, RedisPubSubConfig)) and (
        config.tenant_global
    )


# ....................... #


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

    caches: StrKeyMapping[RedisCacheConfig] | StrKeyMapping[RedisUniversalConfig] | None = (
        attrs.field(
            default=None,
            converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
        )
    )
    """Mapping from cache names to their Redis-specific configurations."""

    counters: StrKeyMapping[RedisCounterConfig] | StrKeyMapping[RedisUniversalConfig] | None = (
        attrs.field(
            default=None,
            converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
        )
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
        StrKeyMapping[RedisSearchResultSnapshotConfig] | StrKeyMapping[RedisUniversalConfig] | None
    ) = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from search snapshot names to their Redis-specific configurations."""

    dlocks: (
        StrKeyMapping[RedisDistributedLockConfig] | StrKeyMapping[RedisUniversalConfig] | None
    ) = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from distributed lock spec names to their Redis-specific configurations."""

    fleets: StrKeyMapping[RedisFleetConfig] | StrKeyMapping[RedisUniversalConfig] | None = (
        attrs.field(
            default=None,
            converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
        )
    )
    """Mapping from fleet spec names to their Redis-specific configurations."""

    revocation_states: StrKeyMapping[RedisRevocationStatesConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from revocation states routes to their configurations: the session fast
    path's cached epochs and states, which every verifier and revoking process reads."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Redis spans: ``tagged`` (per-tenant key prefix via ``tenant_aware``), ``namespace`` (a
    per-tenant ``namespace`` resolver), ``dedicated`` (a routed per-tenant client). Fleet
    routes, and lock and pub/sub routes marked ``tenant_global``, sit outside it.
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

    Registered under ``AckStreamGroupQueryDepKey`` (read/ack/claim/pending) and
    ``AckStreamGroupAdminDepKey`` (group provisioning)."""

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
        # A fleet is the deployment's, not a tenant's: no tenant-scoped route, and outside
        # the floor (a tenant-global group below).
        for name, fleet in (self.fleets or {}).items():
            refuse_tenant_scoped_route(
                str(name),
                tenant_aware=fleet.tenant_aware,
                per_tenant=not is_static_named_resource(fleet.namespace),
            )

        if self.revocation_states and isinstance(self.client, RoutedRedisClient):
            # A routed client connects per tenant, and a verification reads with none bound.
            raise exc.configuration(
                "Redis revocation states need a plain client: this module's client is routed "
                "per tenant, and a token is verified before a tenant is bound.",
                code="revocation_states.routed_client",
            )

        if self.fleets and isinstance(self.client, RoutedRedisClient):
            # A routed client connects per tenant, and the fleet beats with none bound.
            raise exc.configuration(
                "Redis fleet routes need a plain client: this module's client is routed per "
                "tenant, and a fleet beats from lifecycle hooks with no tenant bound. Register "
                "FleetDepKey yourself over a RedisFleetAdapter on a plain RedisClient.",
                code="fleet.routed_client",
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
                    tenant_global=tenant_global,
                )
                for kind, configs, tenant_global in (
                    ("cache", self.caches, _never),
                    ("counter", self.counters, _never),
                    ("idempotency", idempotency_routes, _never),
                    ("search_snapshot", self.search_snapshots, _never),
                    ("dlock", self.dlocks, _marked_tenant_global),
                    # A fleet is the deployment's, whatever tenants its replicas serve.
                    ("fleet", self.fleets, _always),
                    # Revocation states are a principal's, across tenants.
                    ("revocation_states", self.revocation_states, _always),
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
                    tenant_global=tenant_global,
                )
                for kind, configs, tenant_global in (
                    ("stream", self.streams, _never),
                    ("stream_group", self.stream_groups, _never),
                    ("pubsub", self.pubsub, _marked_tenant_global),
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
                    {IdempotencyDepKey: ConfigurableRedisIdempotency(config=self.idempotency)}
                )

        return merge_deps(
            routed_from_mapping(
                self.caches,
                bindings=[(CacheDepKey, ConfigurableRedisCache)],
            ),
            routed_from_mapping(
                self.counters,
                bindings=[
                    (CounterDepKey, ConfigurableRedisCounter),
                    (CounterAdminDepKey, ConfigurableRedisCounterAdmin),
                ],
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
            routed_from_mapping(
                self.fleets,
                bindings=[(FleetDepKey, ConfigurableRedisFleet)],
            ),
            routed_from_mapping(
                self.revocation_states,
                bindings=[(RevocationStatesDepKey, ConfigurableRedisRevocationStates)],
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
                    (AckStreamGroupQueryDepKey, ConfigurableRedisStreamGroup),
                    (AckStreamGroupAdminDepKey, ConfigurableRedisStreamGroupAdmin),
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
