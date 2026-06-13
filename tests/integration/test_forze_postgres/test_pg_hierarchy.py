"""Postgres hierarchy parity: ``$descendant_of`` / ``$ancestor_of`` match the oracle.

Run twice over the same taxonomy — once with the path stored as a native ``ltree`` column
(rendered via the index-backed ``@>`` / ``<@`` containment operators) and once as plain
``text`` (rendered via the ``starts_with`` label-prefix fallback) — to prove both render
paths agree with the in-memory mock and with the hand-pinned label-boundary semantics.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps, ExecutionContext
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.hierarchy import (
    TreeCreate,
    TreeDoc,
    TreeRead,
    assert_hierarchy_parity,
    seed_tree_corpus,
)


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
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


def _spec() -> DocumentSpec:
    return DocumentSpec(
        name="tree",
        read=TreeRead,
        write=DocumentWriteTypes(domain=TreeDoc, create_cmd=TreeCreate),
    )


def _mock_oracle() -> MockDocumentAdapter[Any, Any, Any, Any]:
    return MockDocumentAdapter(
        spec=_spec(),
        state=MockState(),
        namespace="tree",
        read_model=TreeRead,
        domain_model=TreeDoc,
    )


async def _run_parity(pg_client: PostgresClient, table: str, path_type: str) -> None:
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            label text NOT NULL,
            path {path_type} NOT NULL
        );
        """
    )

    ctx = _ctx(pg_client, table)
    spec = _spec()

    await seed_tree_corpus(ctx.document.command(spec))

    oracle = _mock_oracle()
    await seed_tree_corpus(oracle)

    await assert_hierarchy_parity(ctx.document.query(spec), oracle)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hierarchy_postgres_ltree(pg_client: PostgresClient) -> None:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS ltree;")
    await _run_parity(pg_client, f"tree_ltree_{uuid4().hex[:12]}", "ltree")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hierarchy_postgres_text(pg_client: PostgresClient) -> None:
    await _run_parity(pg_client, f"tree_text_{uuid4().hex[:12]}", "text")
