"""Paired system invariants over Postgres: exact ``numeric`` sums and an order with no lines.

Postgres sums a ``numeric`` column to a ``Decimal`` and a ``bigint`` column to ``numeric`` too, so
the value a law receives must follow the read model's declaration, not the column's sum type.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.paired_invariants import LINES, NOTES, ORDERS, assert_paired_laws_on

# ----------------------- #

_BOOKKEEPING = """
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
"""


def _document(table: str) -> ConfigurablePostgresDocument[Any, Any, Any, Any]:
    return ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_paired_laws_over_postgres(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    orders, lines, notes = (f"inv_{name}_{suffix}" for name in ("orders", "lines", "notes"))

    await pg_client.execute(
        f"CREATE TABLE {orders} ({_BOOKKEEPING} qty bigint NOT NULL, total numeric NOT NULL);"
    )
    await pg_client.execute(f"CREATE TABLE {notes} ({_BOOKKEEPING} order_ref text NOT NULL);")
    await pg_client.execute(
        f"""
        CREATE TABLE {lines} (
            {_BOOKKEEPING}
            order_id uuid NOT NULL,
            qty bigint NOT NULL,
            amount numeric NOT NULL,
            kind text NOT NULL
        );
        """
    )

    documents = {
        ORDERS.name: _document(orders),
        LINES.name: _document(lines),
        NOTES.name: _document(notes),
    }
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        ).merge(Deps.routed({DocumentQueryDepKey: documents, DocumentCommandDepKey: documents}))
    )

    await assert_paired_laws_on(ctx)
