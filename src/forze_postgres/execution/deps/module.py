"""Postgres dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.document import DocumentReadDepKey, DocumentWriteDepKey
from forze.application.contracts.search import SearchReadDepKey
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.gateways import PostgresHistoryWriteStrategy, PostgresRevBumpStrategy
from ...kernel.introspect import PostgresIntrospector
from ...kernel.platform import PostgresClient
from .deps import postgres_document_configurable, postgres_search, postgres_txmanager
from .keys import PostgresClientDepKey, PostgresIntrospectorDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDepsModule(DepsModule):
    """Dependency module that registers Postgres client, tx manager, and document port.

    Invoke to produce a :class:`Deps` container with all Postgres-backed
    dependencies. The client must be initialized separately (e.g. via
    :func:`postgres_lifecycle_step`) before usecases run.
    """

    client: PostgresClient
    """Pre-constructed Postgres client (pool not yet initialized)."""

    rev_bump_strategy: PostgresRevBumpStrategy
    """Strategy for revision bumps: ``"database"`` or ``"application"``."""

    history_write_strategy: PostgresHistoryWriteStrategy
    """Strategy for history writes: ``"database"`` or ``"application"``."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Postgres-backed ports.

        :returns: Deps with client, types provider, tx manager, and document port.
        """

        return Deps(
            {
                PostgresClientDepKey: self.client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=self.client),
                TxManagerDepKey: postgres_txmanager,
                SearchReadDepKey: postgres_search,
                DocumentReadDepKey: postgres_document_configurable(
                    rev_bump_strategy=self.rev_bump_strategy,
                    history_write_strategy=self.history_write_strategy,
                ),
                DocumentWriteDepKey: postgres_document_configurable(
                    rev_bump_strategy=self.rev_bump_strategy,
                    history_write_strategy=self.history_write_strategy,
                ),
            }
        )
