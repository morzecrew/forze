"""Redis dependency integration configs (frozen attrs)."""

import attrs

from forze.application.contracts.fleet import refuse_tenant_scoped_route
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    is_static_named_resource,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisUniversalConfig(TenantAwareIntegrationConfig):
    """Base configuration for a Redis resource."""

    namespace: NamedResourceSpec = attrs.field(converter=coerce_named_resource_spec)
    """Namespace for the keys."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisCacheConfig(RedisUniversalConfig):
    """Configuration for a Redis cache."""

    invalidation_push: bool = False
    """Opt-in client-side-caching invalidation push (Redis 6+ ``CLIENT
    TRACKING``): the document L1 drops entries when any replica writes them,
    demoting the L1 TTL to a backstop. Requires a static namespace and a
    non-routed client; unsupported setups silently stay TTL-only."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisCounterConfig(RedisUniversalConfig):
    """Configuration for a Redis counter."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisIdempotencyConfig(RedisUniversalConfig):
    """Configuration for a Redis idempotency store."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisSearchResultSnapshotConfig(RedisUniversalConfig):
    """Configuration for the search result snapshot store."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisDistributedLockConfig(RedisUniversalConfig):
    """Configuration for Redis-backed distributed locks."""

    tenant_global: bool = False
    """The lock is the deployment's, not a tenant's — a singleton or leadership lock, taken
    from lifecycle hooks with no tenant bound. It sits outside the module's tenant-isolation
    floor, and must be neither ``tenant_aware`` nor resolve its namespace per tenant, nor sit
    on a client routed per tenant."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisFleetConfig(RedisUniversalConfig):
    """Configuration for Redis-backed fleet membership.

    Never tenant-scoped: a fleet is one service's replicas, so it takes neither
    ``tenant_aware`` nor a per-tenant namespace, and sits outside the tenant-isolation floor.
    """

    def __attrs_post_init__(self) -> None:
        refuse_tenant_scoped_route(
            "fleet",
            tenant_aware=self.tenant_aware,
            per_tenant=not is_static_named_resource(self.namespace),
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisRevocationStatesConfig(RedisUniversalConfig):
    """Configuration for Redis-backed revocation states.

    Never tenant-scoped: the states are a principal's, and a principal spans tenants; a
    verification also runs before any tenant is bound. Give the states a namespace of their
    own, never shared with data that may be flushed or evicted on its own schedule.
    """

    def __attrs_post_init__(self) -> None:
        if self.tenant_aware or not is_static_named_resource(self.namespace):
            raise exc.configuration(
                "Redis revocation states cannot be tenant-scoped: they belong to a principal, "
                "across tenants, and are read before a tenant is bound. Wire them without "
                "tenant_aware and with a static namespace.",
                code="revocation_states.tenant_scoped_route",
            )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamConfig(TenantAwareIntegrationConfig):
    """Configuration for a Redis stream route (``StreamSpec``).

    No namespace: the stream name is supplied per call (``append`` / ``read``) and, when
    ``tenant_aware``, isolated by the ``tenant:{id}:stream:`` key prefix — there is no
    per-route key namespace to declare."""

    retention_max_entries: int | None = None
    """Approximate cap on the stream's length, applied at every append (``XADD MAXLEN ~``).

    Without a cap a stream grows in Redis memory until an operator intervenes or
    ``maxmemory`` eviction takes the keyspace — set one on any stream with an unbounded
    producer (the realtime egress stream above all). Approximate (``~``) so Redis trims
    whole macro nodes, which is O(1) amortized; the observed length can exceed the cap by
    a node's worth of entries.

    The cap bounds how long an **undelivered** entry survives a total consumer outage:
    size it so the horizon at your peak append rate comfortably exceeds both the consumer
    group's reclaim window and your recovery SLO. Delivery-side lag is the thing to alarm
    on before a cap ever becomes the failure."""

    def __attrs_post_init__(self) -> None:
        if self.retention_max_entries is not None and self.retention_max_entries <= 0:
            raise exc.configuration("retention_max_entries must be positive when set")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamGroupConfig(TenantAwareIntegrationConfig):
    """Configuration for a Redis stream consumer-group route."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisPubSubConfig(TenantAwareIntegrationConfig):
    """Configuration for a Redis pub-sub route (``PubSubSpec``).

    No namespace: the topic is supplied per ``publish`` / ``subscribe`` and, when
    ``tenant_aware``, isolated by the ``tenant:{id}:`` key prefix."""

    tenant_global: bool = False
    """The route carries no tenant's data — an outbox wake, heard by a relay outside any
    tenant. It sits outside the module's tenant-isolation floor, and must be neither
    ``tenant_aware`` nor sit on a client routed per tenant."""
