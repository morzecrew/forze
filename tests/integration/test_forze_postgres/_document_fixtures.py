"""Shared execution-context builders for Postgres document integration tests.

Every document test needs the same two deps registered — the client and an
introspector over it — and most also register a document adapter bound to one
table. Building that inline once per module is how nine byte-identical copies of
the same helper appeared, so it lives here instead, beside
:mod:`._search_fixtures`.
"""

from __future__ import annotations

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


def gateway_context(pg_client: PostgresClient) -> ExecutionContext:
    """Client plus introspector only — for tests that build gateways by hand."""

    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )


# ....................... #


def document_context(pg_client: PostgresClient, table: str) -> ExecutionContext:
    """Gateway context plus a document adapter reading and writing ``public.table``."""

    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )

    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: doc,
                DocumentCommandDepKey: doc,
            }
        )
    )
