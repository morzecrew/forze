"""A blank-query offset page is sorted over every matching row, not over a capped subset.

The ranked pipeline caps how many heap rows it scores, keeping the best-ranked ones. With no
query every row ranks the same, so the cap kept whichever rows the scan reached first, and a
sorted page was the top of that subset: the newest rows of a table larger than the cap never
reached the first page.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderDepKey, EmbeddingsSpec
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_mock import MockHashEmbeddingsProvider
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import FtsEngine, PostgresSearchConfig, VectorEngine
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_ROWS = 80
"""More than the cap plus its page margin, so a capped scan cannot hold every row."""


class _Row(BaseModel):
    id: UUID
    n: int


def _embeddings(_ctx: ExecutionContext, spec: EmbeddingsSpec) -> MockHashEmbeddingsProvider:
    return MockHashEmbeddingsProvider(dimensions=spec.dimensions)


async def _port(client: PostgresClient, *, engine: Any, column: str) -> Any:
    table = f"blank_order_{uuid4().hex[:10]}"

    await client.execute(
        f"CREATE TABLE {table} (id uuid PRIMARY KEY, n int NOT NULL, label text NOT NULL, "
        f"emb {column})"
    )

    # Ascending, so the scan meets the smallest values first.
    for n in range(_ROWS):
        await client.execute(
            f"INSERT INTO {table} (id, n, label, emb) VALUES (%(id)s, %(n)s, 'x', NULL)",
            {"id": uuid4(), "n": n},
        )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=client),
                EmbeddingsProviderDepKey: _embeddings,
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", table),
                        read=("public", table),
                        heap=("public", table),
                        engine=engine,
                        candidate_limit=1,
                    )
                ),
            }
        )
    )

    return ctx.search.query(SearchSpec(name="rows", model_type=_Row, fields=["label"]))


async def _assert_the_page_is_the_global_top(port: Any) -> None:
    page = await port.search_page("", None, {"limit": 2}, {"n": "desc"})

    assert [hit.n for hit in page.hits] == [_ROWS - 1, _ROWS - 2]
    assert page.count == _ROWS


async def test_fts(pg_client: PostgresClient) -> None:
    port = await _port(pg_client, engine=FtsEngine(groups={"A": ("label",)}), column="text")

    await _assert_the_page_is_the_global_top(port)


async def test_vector(pgvector_client: PostgresClient) -> None:
    await pgvector_client.execute("CREATE EXTENSION IF NOT EXISTS vector")
    port = await _port(
        pgvector_client,
        engine=VectorEngine(column="emb", dimensions=3, embeddings_name="vec"),
        column="vector(3)",
    )

    await _assert_the_page_is_the_global_top(port)
