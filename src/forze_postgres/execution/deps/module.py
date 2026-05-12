"""Postgres dependency module for the application kernel."""

from collections.abc import Callable
from datetime import timedelta
from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    HubSearchQueryDepKey,
    SearchQueryDepKey,
)
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.introspect import PostgresIntrospector
from ...kernel.platform import PostgresClientPort
from .configs import (
    PostgresDocumentConfig,
    PostgresFederatedSearchConfig,
    PostgresHubSearchConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
    validate_pg_search_conf,
    validate_postgres_federated_search_conf,
)
from .deps import (
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
class PostgresDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers Postgres clients and adapters."""

    client: PostgresClientPort
    """Pre-constructed Postgres client (single-DSN or routed). For :class:`~forze_postgres.kernel.platform.RoutedPostgresClient`, set :attr:`introspector_cache_partition_key` to match tenant routing."""

    introspector_cache_partition_key: Callable[[], str | None] | None = attrs.field(
        default=None,
    )
    """When set, :class:`PostgresIntrospector` cache keys include this partition (e.g. tenant id).

    Required for correct catalog caching with database-per-tenant routing.
    """

    introspector_cache_ttl: timedelta | None = attrs.field(default=None)
    """Optional TTL for :class:`PostgresIntrospector` catalog caches (``None`` = no expiry)."""

    ro_documents: Mapping[K, PostgresReadOnlyDocumentConfig] | None = attrs.field(
        default=None
    )
    """Mapping from read-only document names to their Postgres-specific configurations."""

    rw_documents: Mapping[K, PostgresDocumentConfig] | None = attrs.field(default=None)
    """Mapping from read-write document names to their Postgres-specific configurations."""

    searches: Mapping[K, PostgresSearchConfig] | None = attrs.field(default=None)
    """Mapping from search names to their Postgres-specific configurations."""

    hub_searches: Mapping[K, PostgresHubSearchConfig] | None = attrs.field(default=None)
    """Mapping from hub search names to their Postgres-specific configurations."""

    federated_searches: Mapping[K, PostgresFederatedSearchConfig] | None = attrs.field(
        default=None,
    )
    """Mapping from federated search names to their Postgres-specific configurations."""

    tx: set[K] | None = attrs.field(default=None)
    """Set of transaction routes to register."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        """Build a dependency container with Postgres-backed ports."""

        plain_deps = Deps[K].plain(
            {
                PostgresClientDepKey: self.client,
                PostgresIntrospectorDepKey: PostgresIntrospector(
                    client=self.client,
                    cache_partition_key=self.introspector_cache_partition_key,
                    cache_ttl=self.introspector_cache_ttl,
                ),
            }
        )

        doc_deps = Deps[K]()
        search_deps = Deps[K]()
        hub_search_deps = Deps[K]()
        federated_search_deps = Deps[K]()
        tx_deps = Deps[K]()

        if self.ro_documents:
            doc_deps = doc_deps.merge(
                Deps[K].routed(
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
                Deps[K].routed(
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
            # fail fast on invalid configurations
            for search_cfg in self.searches.values():
                validate_pg_search_conf(search_cfg)

            search_deps = search_deps.merge(
                Deps[K].routed(
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
                Deps[K].routed(
                    {
                        HubSearchQueryDepKey: {
                            name: ConfigurablePostgresHubSearch(config=config)
                            for name, config in self.hub_searches.items()
                        }
                    }
                )
            )

        if self.federated_searches:
            for federated_cfg in self.federated_searches.values():
                validate_postgres_federated_search_conf(federated_cfg)

            federated_search_deps = federated_search_deps.merge(
                Deps[K].routed(
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
                Deps[K].routed(
                    {TxManagerDepKey: {name: postgres_txmanager for name in self.tx}}
                )
            )

        return plain_deps.merge(
            doc_deps,
            search_deps,
            tx_deps,
            hub_search_deps,
            federated_search_deps,
        )
