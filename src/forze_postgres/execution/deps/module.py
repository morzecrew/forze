"""Postgres dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.document import DocumentReadDepKey, DocumentWriteDepKey
from forze.application.contracts.search import SearchReadDepKey
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.gateways import PostgresBookkeepingStrategy
from ...kernel.introspect import PostgresIntrospector
from ...kernel.platform import PostgresClient
from .configs import (
    PostgresDocumentConfigs,
    PostgresSearchConfigs,
    validate_pg_search_conf,
)
from .deps import (
    ConfigurablePostgresDocument,
    ConfigurablePostgresSearch,
    postgres_txmanager,
)
from .keys import PostgresClientDepKey, PostgresIntrospectorDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDepsModule(DepsModule):
    """Dependency module that registers Postgres client, introspector, tx manager, search index, and document read and write adapters.

    Invoke to produce a :class:`Deps` container with all Postgres-backed
    dependencies. The client must be initialized separately (e.g. via
    :func:`postgres_lifecycle_step`) before usecases run.
    """

    client: PostgresClient
    """Pre-constructed Postgres client (pool not yet initialized)."""

    bookkeeping_strategy: PostgresBookkeepingStrategy
    """Strategy for bookkeeping: ``"database"`` or ``"application"``."""

    document_configs: PostgresDocumentConfigs = attrs.field(factory=dict)
    """Mapping from document names to their Postgres-specific configurations."""

    search_configs: PostgresSearchConfigs = attrs.field(factory=dict)
    """Mapping from search names to their Postgres-specific configurations."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Postgres-backed ports.

        :returns: Deps with client, types provider, tx manager, and document port.
        """

        if self.search_configs:
            for cfg in self.search_configs.values():
                validate_pg_search_conf(cfg)

        return Deps(
            {
                PostgresClientDepKey: self.client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=self.client),
                TxManagerDepKey: postgres_txmanager,
                SearchReadDepKey: ConfigurablePostgresSearch(
                    configs=self.search_configs,
                ),
                DocumentReadDepKey: ConfigurablePostgresDocument(
                    bookkeeping_strategy=self.bookkeeping_strategy,
                    configs=self.document_configs,
                ),
                DocumentWriteDepKey: ConfigurablePostgresDocument(
                    bookkeeping_strategy=self.bookkeeping_strategy,
                    configs=self.document_configs,
                ),
            }
        )
