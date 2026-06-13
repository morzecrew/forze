"""Postgres ``$having`` parity: post-group filtering matches the in-memory oracle.

Postgres wraps the group query in a subquery and filters its output aliases; this checks
the result against the mock for count/sum thresholds, multi-key groups, and a group-key +
metric mix.
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
from tests.support.aggregate_having import (
    AggCreate,
    AggDoc,
    AggRead,
    assert_aggregate_having_parity,
    seed_aggregate_corpus,
)
from tests.support.execution_context import context_from_deps


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


def _mock_oracle() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="agg",
        read=AggRead,
        write=DocumentWriteTypes(domain=AggDoc, create_cmd=AggCreate),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="agg",
        read_model=AggRead,
        domain_model=AggDoc,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aggregate_having_postgres(pg_client: PostgresClient) -> None:
    t = f"agg_having_{uuid4().hex[:12]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            region text NOT NULL,
            tier text NOT NULL,
            amount integer NOT NULL
        );
        """
    )

    spec = DocumentSpec(
        name="agg",
        read=AggRead,
        write=DocumentWriteTypes(domain=AggDoc, create_cmd=AggCreate),
    )
    ctx = _ctx(pg_client, t)

    await seed_aggregate_corpus(ctx.document.command(spec))

    oracle = _mock_oracle()
    await seed_aggregate_corpus(oracle)

    await assert_aggregate_having_parity(ctx.document.query(spec), oracle)
