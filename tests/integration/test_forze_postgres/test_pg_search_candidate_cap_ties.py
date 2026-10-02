"""A ranked search's candidate cap keeps the first rows of the page order.

The cap kept the best-ranked rows, ordered by rank alone. Rows that tie on rank at the cap's
edge were then kept in whatever order the scan met them, so the pool, and every page sorted
out of it, could differ from one request to the next and from the uncapped cursor. The cap now
orders as the page does, so a page cut from it is the page the uncapped order holds.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderDepKey, EmbeddingsSpec
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_mock import MockHashEmbeddingsProvider
from forze_postgres.adapters.search._vector_sql import vector_param_literal
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PgroongaEngine,
    PostgresSearchConfig,
    VectorEngine,
)
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_ROWS = 80


class _Row(BaseModel):
    id: UUID
    n: int


def _embeddings(_ctx: ExecutionContext, spec: EmbeddingsSpec) -> MockHashEmbeddingsProvider:
    return MockHashEmbeddingsProvider(dimensions=spec.dimensions)


async def _seed(client: PostgresClient, *, column: str, emb: str) -> str:
    table = f"cap_ties_{uuid4().hex[:10]}"
    await client.execute(
        f"CREATE TABLE {table} (id uuid PRIMARY KEY, n int NOT NULL, label text NOT NULL, "
        f"emb {column})"
    )
    await client.execute(
        f"CREATE INDEX {table}_fts ON {table} USING gin (to_tsvector('english', label))"
    )

    for n in range(_ROWS):
        await client.execute(
            f"INSERT INTO {table} (id, n, label, emb) VALUES (%(id)s, %(n)s, 'x', {emb})",
            {"id": uuid4(), "n": n},
        )

    return table


def _port(client: Any, table: str, engine: Any, index: str) -> Any:
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=client),
                EmbeddingsProviderDepKey: _embeddings,
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index),
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


async def _expected(client: PostgresClient, table: str) -> list[int]:
    """The page the uncapped order holds: every row ties on rank, so the top two by ``n``."""

    rows = await client.fetch_all(
        sql.SQL("SELECT n FROM {} ORDER BY n DESC LIMIT 2").format(sql.Identifier(table))
    )

    return [int(row["n"]) for row in rows]


@pytest.mark.parametrize("plan", ["filter_first", "index_first"])
async def test_pgroonga(pg_client: PostgresClient, plan: str) -> None:
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    table = await _seed(pg_client, column="text", emb="NULL")
    await pg_client.execute(f"CREATE INDEX {table}_pgr ON {table} USING pgroonga (label)")
    port = _port(pg_client, table, PgroongaEngine(plan=plan), f"{table}_pgr")  # type: ignore[arg-type]

    page = await port.search_page("x", None, {"limit": 2}, {"n": "desc"})

    assert [hit.n for hit in page.hits] == await _expected(pg_client, table)


async def test_fts(pg_client: PostgresClient) -> None:
    table = await _seed(pg_client, column="text", emb="NULL")
    port = _port(pg_client, table, FtsEngine(groups={"A": ("label",)}), f"{table}_fts")

    page = await port.search_page("x", None, {"limit": 2}, {"n": "desc"})

    assert [hit.n for hit in page.hits] == await _expected(pg_client, table)


async def test_vector(pgvector_client: PostgresClient) -> None:
    await pgvector_client.execute("CREATE EXTENSION IF NOT EXISTS vector")
    same = vector_param_literal(await MockHashEmbeddingsProvider(dimensions=3).embed_one("x"))
    table = await _seed(pgvector_client, column="vector(3) NOT NULL", emb=f"'{same}'::vector")
    port = _port(
        pgvector_client,
        table,
        VectorEngine(column="emb", dimensions=3, embeddings_name="vec"),
        table,
    )

    page = await port.search_page("x", None, {"limit": 2}, {"n": "desc"})

    assert [hit.n for hit in page.hits] == await _expected(pgvector_client, table)


async def test_a_capped_vector_search_keeps_the_nearest_rows(
    pgvector_client: PostgresClient,
) -> None:
    # The score is the negated distance, higher nearer; the cap kept the lowest scores.
    await pgvector_client.execute("CREATE EXTENSION IF NOT EXISTS vector")
    embedder = MockHashEmbeddingsProvider(dimensions=3)
    table = f"cap_near_{uuid4().hex[:10]}"
    await pgvector_client.execute(
        f"CREATE TABLE {table} (id uuid PRIMARY KEY, n int NOT NULL, label text NOT NULL, "
        "emb vector(3) NOT NULL)"
    )

    for n in range(_ROWS):
        emb = vector_param_literal(await embedder.embed_one(f"label {n}"))
        await pgvector_client.execute(
            f"INSERT INTO {table} (id, n, label, emb) VALUES (%(id)s, %(n)s, 'x', '{emb}'::vector)",
            {"id": uuid4(), "n": n},
        )

    port = _port(
        pgvector_client,
        table,
        VectorEngine(column="emb", dimensions=3, embeddings_name="vec"),
        table,
    )
    page = await port.search_page("label 7", None, {"limit": 1})

    assert [hit.n for hit in page.hits] == [7]


@pytest.mark.parametrize(
    "engine",
    [
        FtsEngine(groups={"A": ("label",)}),
        PgroongaEngine(plan="filter_first"),
        PgroongaEngine(plan="index_first"),
    ],
    ids=["fts", "pgroonga-filter-first", "pgroonga-index-first"],
)
async def test_a_projection_apart_from_the_heap(pg_client: PostgresClient, engine: Any) -> None:
    # The sort column lives on the projection, which the capped CTE reaches only through the
    # filtered CTE; index-first, which caps before the projection, runs filter-first instead.
    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    table = await _seed(pg_client, column="text", emb="NULL")
    view = f"{table}_v"
    await pg_client.execute(f"CREATE VIEW {view} AS SELECT id, n, label, emb FROM {table}")
    index = f"{table}_fts"

    if isinstance(engine, PgroongaEngine):
        index = f"{table}_pgr"
        await pg_client.execute(f"CREATE INDEX {index} ON {table} USING pgroonga (label)")

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index),
                        read=("public", view),
                        heap=("public", table),
                        engine=engine,
                        candidate_limit=1,
                    )
                ),
            }
        )
    )
    port = ctx.search.query(SearchSpec(name="rows", model_type=_Row, fields=["label"]))

    page = await port.search_page("x", None, {"limit": 2}, {"n": "desc"})

    assert [hit.n for hit in page.hits] == await _expected(pg_client, table)
