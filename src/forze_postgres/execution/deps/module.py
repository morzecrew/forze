"""Postgres dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.document import (
    DocumentReadDepKey,
    DocumentWriteDepKey,
)
from forze.application.contracts.search import SearchReadDepKey
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.introspect import PostgresIntrospector
from ...kernel.platform import PostgresClient
from .configs import (
    PostgresDocumentConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
    validate_pg_search_conf,
)
from .deps import (
    ConfigurablePostgresDocument,
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

    client: PostgresClient
    """Pre-constructed Postgres client (pool not yet initialized)."""

    ro_documents: dict[str, PostgresReadOnlyDocumentConfig] = attrs.field(factory=dict)
    """Mapping from read-only document names to their Postgres-specific configurations."""

    rw_documents: dict[str, PostgresDocumentConfig] = attrs.field(factory=dict)
    """Mapping from read-write document names to their Postgres-specific configurations."""

    searches: dict[str, PostgresSearchConfig] = attrs.field(factory=dict)
    """Mapping from search names to their Postgres-specific configurations."""

    tx: set[str] = attrs.field(factory=set)
    """Set of transaction routes to register."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Postgres-backed ports."""

        plain_deps = Deps.plain(
            {
                PostgresClientDepKey: self.client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=self.client),
            }
        )

        doc_deps = Deps()
        search_deps = Deps()
        tx_deps = Deps()

        if self.ro_documents:
            doc_deps = doc_deps.merge(
                Deps.routed(
                    {
                        DocumentReadDepKey: {
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
                        DocumentReadDepKey: {
                            name: ConfigurablePostgresReadOnlyDocument(config=config)
                            for name, config in self.ro_documents.items()
                        },
                        DocumentWriteDepKey: {
                            name: ConfigurablePostgresDocument(config=config)
                            for name, config in self.rw_documents.items()
                        },
                    }
                ),
            )

        if self.searches:
            # fail fast on invalid configurations
            for cfg in self.searches.values():
                validate_pg_search_conf(cfg)

            search_deps = search_deps.merge(
                Deps.routed(
                    {
                        SearchReadDepKey: {
                            name: ConfigurablePostgresSearch(config=config)
                            for name, config in self.searches.items()
                        }
                    }
                )
            )

        if self.tx:
            tx_deps = tx_deps.merge(
                Deps.routed(
                    {TxManagerDepKey: {name: postgres_txmanager for name in self.tx}}
                )
            )

        return plain_deps.merge(doc_deps, search_deps, tx_deps)
