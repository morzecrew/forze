"""Postgres search aggregates: the paths the shared conformance battery cannot reach.

The battery runs both text engines on one table. Here: a capped index-first PGroonga plan,
a read projection apart from the index heap, parameters from the query, the filter, a metric
filter, ``$having`` and the page window in one statement, and the vector engine refusing.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderDepKey, EmbeddingsSpec
from forze.application.contracts.querying import UNSUPPORTED_QUERY_FEATURE_CODE
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze_mock import MockHashEmbeddingsProvider
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

# ----------------------- #


class _Row(BaseModel):
    id: UUID
    title: str
    content: str
    category: str
    amount: Decimal


_ROWS = (
    ("ledger one", "x", "a", Decimal("10.10")),
    ("ledger two", "x", "a", Decimal("20.20")),
    ("ledger three", "x", "a", Decimal("30.30")),
    ("ledger four", "x", "b", Decimal("40.40")),
    ("ledger five", "x", "b", Decimal("50.50")),
    ("unrelated", "x", "c", Decimal("99.99")),
)
"""Five rows match ``ledger`` (three in ``a``, two in ``b``); one in ``c`` matches nothing."""

_INDEXES = {
    "fts": (
        "USING gin (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content, '')))"
    ),
    "pgroonga": "USING pgroonga ((ARRAY[title, content]))",
}

_BY_CATEGORY: dict[str, Any] = {
    "$groups": {"category": "category"},
    "$computed": {"n": {"$count": None}, "total": {"$sum": "amount"}},
}


async def _table(pg_client: PostgresClient, engine: str) -> tuple[str, str]:
    if engine == "pgroonga":
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    tag = uuid4().hex[:12]
    table, index = f"sagg_{engine}_{tag}", f"idx_sagg_{engine}_{tag}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            content text NOT NULL,
            category text NOT NULL,
            amount numeric NOT NULL
        );
        CREATE INDEX {index} ON {table} {_INDEXES[engine]};
        """
    )

    for title, content, category, amount in _ROWS:
        await pg_client.execute(
            f"INSERT INTO {table} (id, title, content, category, amount) "
            "VALUES (%(id)s, %(t)s, %(c)s, %(k)s, %(a)s)",
            {"id": uuid4(), "t": title, "c": content, "k": category, "a": amount},
        )

    return table, index


def _port(pg_client: PostgresClient, engine: object, **config: Any) -> Any:
    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(engine=engine, **config)  # type: ignore[arg-type]
                ),
            }
        )
    )

    return ctx.search.query(
        SearchSpec(name=f"sagg_{uuid4().hex[:8]}", model_type=_Row, fields=["title", "content"])
    )


def _engine(name: str) -> object:
    return FtsEngine(groups={"A": ("title",), "B": ("content",)}) if name == "fts" else "pgroonga"


# ....................... #


@pytest.mark.asyncio
async def test_a_capped_route_aggregates_every_match(
    pg_client: PostgresClient,
) -> None:
    """A page reads the top two candidates; the aggregate reads all five matches.

    Configured ``index_first``, whose heap ``LIMIT`` is a cap: the aggregate's uncapped read
    takes the filter-first plan instead, as a cursor walk does.
    """

    table, index = await _table(pg_client, "pgroonga")
    port = _port(
        pg_client,
        PgroongaEngine(plan="index_first"),
        index=("public", index),
        read=("public", table),
        candidate_limit=2,
    )

    page = await port.aggregate_search_page(_BY_CATEGORY, "ledger")

    assert {row["category"]: (row["n"], row["total"]) for row in page.hits} == {
        "a": (3, Decimal("60.60")),
        "b": (2, Decimal("90.90")),
    }
    assert page.count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("engine", ["fts", "pgroonga"])
async def test_every_clause_binds_its_own_parameters(
    pg_client: PostgresClient, engine: str
) -> None:
    """Query, filter, metric filter, ``$having`` and window parameters, in one statement."""

    table, index = await _table(pg_client, engine)
    port = _port(pg_client, _engine(engine), index=("public", index), read=("public", table))
    aggregates = {
        "$groups": {"category": "category"},
        "$computed": {
            "n": {"$count": None},
            "big": {"$count": {"filter": {"$values": {"amount": {"$gte": Decimal("30")}}}}},
        },
        "$having": {"$values": {"n": {"$gte": 2}}},
    }

    page = await port.aggregate_search_page(
        aggregates,
        "ledger",
        {"$values": {"amount": {"$gte": Decimal("20")}}},
        {"limit": 1, "offset": 1},
        {"category": "asc"},
    )

    # Filtered: a has 20.20 and 30.30, b has 40.40 and 50.50; both keep n >= 2.
    assert page.count == 2
    assert page.hits == [{"category": "b", "n": 2, "big": 2}]


@pytest.mark.asyncio
@pytest.mark.parametrize("engine", ["fts", "pgroonga"])
async def test_a_separate_projection_is_what_gets_measured(
    pg_client: PostgresClient, engine: str
) -> None:
    """The read relation, not the index heap, holds the values an aggregate sums."""

    table, index = await _table(pg_client, engine)
    view = f"{table}_v"
    await pg_client.execute(
        f"CREATE VIEW {view} AS "
        f"SELECT id, title, content, category, amount * 2 AS amount FROM {table}"
    )
    port = _port(
        pg_client,
        _engine(engine),
        index=("public", index),
        read=("public", view),
        heap=("public", table),
    )

    page = await port.aggregate_search(_BY_CATEGORY, "ledger", None, None, {"category": "asc"})

    assert [(row["category"], row["total"]) for row in page.hits] == [
        ("a", Decimal("121.20")),
        ("b", Decimal("181.80")),
    ]


@pytest.mark.asyncio
async def test_vector_search_refuses_to_aggregate(pgvector_client: PostgresClient) -> None:
    """Every row is a neighbour of an embedding, so a vector "match" has no set to measure."""

    await pgvector_client.execute("CREATE EXTENSION IF NOT EXISTS vector")
    tag = uuid4().hex[:12]
    table = f"sagg_vec_{tag}"
    await pgvector_client.execute(
        f"CREATE TABLE {table} (id uuid PRIMARY KEY, title text NOT NULL, "
        "content text NOT NULL, category text NOT NULL, amount numeric NOT NULL, "
        "emb vector(3) NOT NULL)"
    )

    def _embeddings(_ctx: ExecutionContext, spec: EmbeddingsSpec) -> MockHashEmbeddingsProvider:
        return MockHashEmbeddingsProvider(dimensions=spec.dimensions)

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pgvector_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pgvector_client),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", f"idx_{table}"),
                        read=("public", table),
                        heap=("public", table),
                        engine=VectorEngine(column="emb", dimensions=3, embeddings_name="sagg"),
                    )
                ),
                EmbeddingsProviderDepKey: _embeddings,
            }
        )
    )
    port = ctx.search.query(
        SearchSpec(name=f"sagg_vec_{tag}", model_type=_Row, fields=["title", "content"])
    )

    with pytest.raises(CoreException) as refused:
        await port.aggregate_search(_BY_CATEGORY, "ledger")

    assert refused.value.code == UNSUPPORTED_QUERY_FEATURE_CODE


@pytest.mark.asyncio
@pytest.mark.parametrize("engine", ["fts", "pgroonga"])
async def test_a_blank_query_measures_the_rows_its_page_counts(
    pg_client: PostgresClient, engine: str
) -> None:
    """With a projection holding a row the index heap lacks, each engine's blank page counts
    its own set; the aggregate measures that same set."""

    table, index = await _table(pg_client, engine)
    view = f"{table}_v"
    await pg_client.execute(
        f"CREATE VIEW {view} AS SELECT id, title, content, category, amount FROM {table} "
        "UNION ALL SELECT gen_random_uuid(), 'extra', 'x', 'd', 1"
    )
    port = _port(
        pg_client,
        _engine(engine),
        index=("public", index),
        read=("public", view),
        heap=("public", table),
    )
    count = {"$computed": {"n": {"$count": None}}}

    for filters in (None, {"$values": {"category": {"$neq": "c"}}}):
        page = await port.search_page("", filters, {"limit": 1})
        measured = await port.aggregate_search(count, "", filters)

        assert [row["n"] for row in measured.hits] == [page.count], (engine, filters)
