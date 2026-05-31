"""Postgres dependency module for the application kernel."""

from datetime import timedelta
from typing import Callable, Mapping, final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    HubSearchQueryDepKey,
    SearchQueryDepKey,
)
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, DepsModule
from forze.base.primitives import StrKey

from ...kernel.catalog.introspect import PostgresIntrospector
from ...kernel.catalog.validation.validate_relation_specs import (
    warn_dynamic_relation_with_tenant_aware,
)
from ...kernel.catalog.validation.validate_tenancy import (
    PostgresTenancyRouteSpec,
    validate_postgres_tenancy_wiring,
)
from ...kernel.client import PostgresClientPort, RoutedPostgresClient
from .configs import (
    PostgresAnalyticsConfig,
    PostgresDocumentConfig,
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLegHub,
    PostgresHubSearchConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
)
from .factories import (
    ConfigurablePostgresAnalytics,
    ConfigurablePostgresDocument,
    ConfigurablePostgresFederatedSearch,
    ConfigurablePostgresHubSearch,
    ConfigurablePostgresReadOnlyDocument,
    ConfigurablePostgresSearch,
    postgres_txmanager,
)
from .keys import PostgresClientDepKey, PostgresIntrospectorDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDepsModule(DepsModule):
    """Dependency module that registers Postgres clients and adapters."""

    client: PostgresClientPort
    """Pre-constructed Postgres client (single-DSN or routed). For :class:`~forze_postgres.kernel.client.RoutedPostgresClient`, set :attr:`introspector_cache_partition_key` to match tenant routing."""

    introspector_cache_partition_key: Callable[[], str | None] | None = attrs.field(
        default=None,
    )
    """When set, :class:`PostgresIntrospector` cache keys include this partition (e.g. tenant id).

    Required for correct catalog caching with database-per-tenant routing.
    """

    introspector_cache_ttl: timedelta | None = attrs.field(default=None)
    """Optional TTL for :class:`PostgresIntrospector` catalog caches (``None`` = no expiry)."""

    ro_documents: Mapping[StrKey, PostgresReadOnlyDocumentConfig] | None = attrs.field(
        default=None
    )
    """Mapping from read-only document names to their Postgres-specific configurations."""

    rw_documents: Mapping[StrKey, PostgresDocumentConfig] | None = attrs.field(
        default=None
    )
    """Mapping from read-write document names to their Postgres-specific configurations."""

    searches: Mapping[StrKey, PostgresSearchConfig] | None = attrs.field(default=None)
    """Mapping from search names to their Postgres-specific configurations."""

    hub_searches: Mapping[StrKey, PostgresHubSearchConfig] | None = attrs.field(
        default=None
    )
    """Mapping from hub search names to their Postgres-specific configurations."""

    federated_searches: Mapping[StrKey, PostgresFederatedSearchConfig] | None = (
        attrs.field(
            default=None,
        )
    )
    """Mapping from federated search names to their Postgres-specific configurations."""

    tx: set[StrKey] | None = attrs.field(default=None)
    """Set of transaction routes to register."""

    analytics: Mapping[StrKey, PostgresAnalyticsConfig] | None = attrs.field(
        default=None
    )
    """Mapping from analytics route names to their Postgres-specific configurations."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        routes: list[PostgresTenancyRouteSpec] = []

        if self.ro_documents:
            for name, cfg in self.ro_documents.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=cfg.tenant_aware,
                        kind="document",
                    ),
                )
                warn_dynamic_relation_with_tenant_aware(
                    route_name=str(name),
                    kind="document",
                    tenant_aware=cfg.tenant_aware,
                    fields=[("read", cfg.read)],
                )

        if self.rw_documents:
            for name, cfg in self.rw_documents.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=cfg.tenant_aware,
                        kind="document",
                    ),
                )
                warn_dynamic_relation_with_tenant_aware(
                    route_name=str(name),
                    kind="document",
                    tenant_aware=cfg.tenant_aware,
                    fields=[
                        ("read", cfg.read),
                        ("write", cfg.write),
                        ("history", cfg.history),
                    ],
                )

        if self.searches:
            for name, search_cfg in self.searches.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=search_cfg.tenant_aware,
                        kind="search",
                    ),
                )
                warn_dynamic_relation_with_tenant_aware(
                    route_name=str(name),
                    kind="search",
                    tenant_aware=search_cfg.tenant_aware,
                    fields=[
                        ("index", search_cfg.index),
                        ("read", search_cfg.read),
                        ("heap", search_cfg.heap_relation),
                    ],
                )

        if self.hub_searches:
            for name, hub_search_cfg in self.hub_searches.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=hub_search_cfg.tenant_aware,
                        kind="hub_search",
                    ),
                )
                warn_dynamic_relation_with_tenant_aware(
                    route_name=str(name),
                    kind="hub_search",
                    tenant_aware=hub_search_cfg.tenant_aware,
                    fields=[("hub", hub_search_cfg.hub)],
                )

                for member_name, leg in hub_search_cfg.members.items():
                    warn_dynamic_relation_with_tenant_aware(
                        route_name=f"{name}.{member_name}",
                        kind="hub_search",
                        tenant_aware=hub_search_cfg.tenant_aware,
                        fields=[
                            ("index", leg.index),
                            ("read", leg.read),
                            ("heap", leg.heap_relation),
                        ],
                    )

        if self.federated_searches:
            for name, federated_search_cfg in self.federated_searches.items():
                routes.append(
                    PostgresTenancyRouteSpec(
                        name=str(name),
                        tenant_aware=federated_search_cfg.tenant_aware,
                        kind="federated_search",
                    ),
                )

                for member_name, fed_leg in federated_search_cfg.members.items():
                    if isinstance(fed_leg, PostgresFederatedSearchLegHub):
                        hub_cfg = fed_leg.hub
                        warn_dynamic_relation_with_tenant_aware(
                            route_name=f"{name}.{member_name}",
                            kind="federated_search",
                            tenant_aware=federated_search_cfg.tenant_aware,
                            fields=[("hub", hub_cfg.hub)],
                        )

                        for hub_member_name, hub_leg in hub_cfg.members.items():
                            warn_dynamic_relation_with_tenant_aware(
                                route_name=f"{name}.{member_name}.{hub_member_name}",
                                kind="federated_search",
                                tenant_aware=federated_search_cfg.tenant_aware,
                                fields=[
                                    ("index", hub_leg.index),
                                    ("read", hub_leg.read),
                                    ("heap", hub_leg.heap_relation),
                                ],
                            )

                    else:
                        search_cfg = fed_leg.search

                        warn_dynamic_relation_with_tenant_aware(
                            route_name=f"{name}.{member_name}",
                            kind="federated_search",
                            tenant_aware=federated_search_cfg.tenant_aware,
                            fields=[
                                ("index", search_cfg.index),
                                ("read", search_cfg.read),
                                ("heap", search_cfg.heap_relation),
                            ],
                        )

        validate_postgres_tenancy_wiring(
            client_is_routed=isinstance(self.client, RoutedPostgresClient),
            introspector_cache_partition_key_set=(
                self.introspector_cache_partition_key is not None
            ),
            routes=routes,
        )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Postgres-backed ports."""

        plain_deps = Deps.plain(
            {
                PostgresClientDepKey: self.client,
                PostgresIntrospectorDepKey: PostgresIntrospector(
                    client=self.client,
                    cache_partition_key=self.introspector_cache_partition_key,
                    cache_ttl=self.introspector_cache_ttl,
                ),
            }
        )

        doc_deps = Deps()
        search_deps = Deps()
        hub_search_deps = Deps()
        federated_search_deps = Deps()
        tx_deps = Deps()
        analytics_deps = Deps()

        if self.ro_documents:
            doc_deps = doc_deps.merge(
                Deps.routed(
                    {
                        DocumentQueryDepKey: {
                            name: ConfigurablePostgresReadOnlyDocument(config=config)
                            for name, config in self.ro_documents.items()
                        }
                    }
                )
            )

        if self.rw_documents:
            doc_deps = doc_deps.merge(
                Deps.routed(
                    {
                        DocumentQueryDepKey: {
                            name: ConfigurablePostgresReadOnlyDocument(config=config)
                            for name, config in self.rw_documents.items()
                        },
                        DocumentCommandDepKey: {
                            name: ConfigurablePostgresDocument(config=config)
                            for name, config in self.rw_documents.items()
                        },
                    }
                ),
            )

        if self.searches:
            search_deps = search_deps.merge(
                Deps.routed(
                    {
                        SearchQueryDepKey: {
                            name: ConfigurablePostgresSearch(config=config)
                            for name, config in self.searches.items()
                        }
                    }
                )
            )

        if self.hub_searches:
            hub_search_deps = hub_search_deps.merge(
                Deps.routed(
                    {
                        HubSearchQueryDepKey: {
                            name: ConfigurablePostgresHubSearch(config=config)
                            for name, config in self.hub_searches.items()
                        }
                    }
                )
            )

        if self.federated_searches:
            federated_search_deps = federated_search_deps.merge(
                Deps.routed(
                    {
                        FederatedSearchQueryDepKey: {
                            name: ConfigurablePostgresFederatedSearch(config=config)
                            for name, config in self.federated_searches.items()
                        }
                    }
                )
            )

        if self.tx:
            tx_deps = tx_deps.merge(
                Deps.routed(
                    {
                        TransactionManagerDepKey: {
                            name: postgres_txmanager for name in self.tx
                        }
                    }
                )
            )

        if self.analytics:
            factory = ConfigurablePostgresAnalytics
            analytics_deps = analytics_deps.merge(
                Deps.routed(
                    {
                        AnalyticsQueryDepKey: {
                            name: factory(config=config)
                            for name, config in self.analytics.items()
                        },
                        AnalyticsIngestDepKey: {
                            name: factory(config=config)
                            for name, config in self.analytics.items()
                        },
                    }
                )
            )

        return plain_deps.merge(
            doc_deps,
            search_deps,
            tx_deps,
            hub_search_deps,
            federated_search_deps,
            analytics_deps,
        )
