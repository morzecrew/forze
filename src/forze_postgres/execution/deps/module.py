"""Postgres dependency module for the application kernel."""

from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.search import SearchQueryDepKey
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


def _document_config_to_read_only(
    config: PostgresDocumentConfig,
) -> PostgresReadOnlyDocumentConfig:
    """Derive a read-only config from a read-write document config (same ``read`` relation)."""

    ro: PostgresReadOnlyDocumentConfig = {"read": config["read"]}

    if "tenant_aware" in config:
        ro["tenant_aware"] = config["tenant_aware"]

    return ro


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers Postgres clients and adapters."""

    client: PostgresClient
    """Pre-constructed Postgres client (pool not yet initialized)."""

    ro_documents: Mapping[K, PostgresReadOnlyDocumentConfig] | None = None
    """Mapping from read-only document names to their Postgres-specific configurations."""

    rw_documents: Mapping[K, PostgresDocumentConfig] | None = None
    """Mapping from read-write document names to their Postgres-specific configurations."""

    searches: Mapping[K, PostgresSearchConfig] | None = None
    """Mapping from search names to their Postgres-specific configurations."""

    tx: set[K] | None = None
    """Set of transaction routes to register."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        """Build a dependency container with Postgres-backed ports."""

        plain_deps = Deps[K].plain(
            {
                PostgresClientDepKey: self.client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=self.client),
            }
        )

        doc_deps = Deps[K]()
        search_deps = Deps[K]()
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
                            name: ConfigurablePostgresReadOnlyDocument(
                                config=_document_config_to_read_only(config)
                            )
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
            for cfg in self.searches.values():
                validate_pg_search_conf(cfg)

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

        if self.tx:
            tx_deps = tx_deps.merge(
                Deps[K].routed(
                    {TxManagerDepKey: {name: postgres_txmanager for name in self.tx}}
                )
            )

        return plain_deps.merge(doc_deps, search_deps, tx_deps)
