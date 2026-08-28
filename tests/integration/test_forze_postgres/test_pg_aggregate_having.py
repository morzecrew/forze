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
    DocumentSpec,
    DocumentWriteTypes,
)
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.support.aggregate_functions import assert_aggregate_function_parity
from tests.support.aggregate_having import (
    AggCreate,
    AggDoc,
    AggRead,
    assert_aggregate_having_parity,
    seed_aggregate_corpus,
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
    ctx = document_context(pg_client, t)

    await seed_aggregate_corpus(ctx.document.command(spec))

    oracle = _mock_oracle()
    await seed_aggregate_corpus(oracle)

    await assert_aggregate_having_parity(ctx.document.query(spec), oracle)
    # Postgres percentile_cont is exact, so all functions are value-checked.
    await assert_aggregate_function_parity(
        ctx.document.query(spec), oracle, exclude_approx=False
    )
