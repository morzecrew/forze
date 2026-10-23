"""A ranked search whose read relation is its heap reads a view once (integration).

The ranked CTE carries the page's columns, so the outer query no longer joins the view back to
itself by id: Postgres expands and plans the view once. The page matches the same ranking
written by hand, row for row and score for score.
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
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import PostgresSearchConfig, VectorEngine
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.integration


class ProductRow(BaseModel):
    id: UUID
    title: str
    product_type: str
    color: str


class _Recording:
    """Delegates to the real client and keeps every ``fetch_all`` statement and its params."""

    def __init__(self, inner: PostgresClient) -> None:
        self._inner = inner
        self.fetched: list[tuple[Any, Any]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def fetch_all(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
        self.fetched.append((query, params))
        return await self._inner.fetch_all(query, params, **kwargs)


def _relations(plan: Any) -> list[str]:
    found: list[str] = []
    stack = [plan]

    while stack:
        node = stack.pop()

        if isinstance(node, dict):
            if "Relation Name" in node:
                found.append(node["Relation Name"])
            stack.extend(node.values())

        elif isinstance(node, list):
            stack.extend(node)

    return found


async def test_a_view_is_read_once_and_ranks_as_written_by_hand(
    pgvector_client: PostgresClient,
) -> None:
    await pgvector_client.execute("CREATE EXTENSION IF NOT EXISTS vector")
    s = uuid4().hex[:10]
    groups, embeddings, profiles, view = (
        f"pg_{s}",
        f"pe_{s}",
        f"pp_{s}",
        f"pv_{s}",
    )
    await pgvector_client.execute(
        f"""
        CREATE TABLE {groups} (id uuid PRIMARY KEY, title text NOT NULL, product_type text NOT NULL);
        CREATE TABLE {embeddings} (group_id uuid PRIMARY KEY REFERENCES {groups}(id),
                                   embedding vector(3) NOT NULL);
        CREATE TABLE {profiles} (id uuid PRIMARY KEY REFERENCES {groups}(id), color text NOT NULL);
        CREATE VIEW {view} AS
            SELECT g.id, g.title, g.product_type, p.color, e.embedding
            FROM {groups} g
            JOIN {embeddings} e ON e.group_id = g.id
            JOIN {profiles} p ON p.id = g.id;
        """
    )
    types = ("shirt", "shoe")
    colors = ("red", "blue")

    for i in range(24):
        gid = uuid4()
        vec = f"[{1 + i % 5},{(i * 7) % 11 + 1},{(i * 3) % 4 + 1}]"
        await pgvector_client.execute(
            f"INSERT INTO {groups} VALUES (%(id)s, %(title)s, %(type)s)",
            {"id": gid, "title": f"item-{i}", "type": types[i % 2]},
        )
        await pgvector_client.execute(
            f"INSERT INTO {embeddings} VALUES (%(id)s, %(vec)s::vector)",
            {"id": gid, "vec": vec},
        )
        await pgvector_client.execute(
            f"INSERT INTO {profiles} VALUES (%(id)s, %(color)s)",
            {"id": gid, "color": colors[i % 3 % 2]},
        )

    client = _Recording(pgvector_client)

    def _embeddings(_ctx: ExecutionContext, spec: EmbeddingsSpec) -> MockHashEmbeddingsProvider:
        return MockHashEmbeddingsProvider(dimensions=spec.dimensions)

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pgvector_client),
                EmbeddingsProviderDepKey: _embeddings,
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", view),
                        read=("public", view),
                        engine=VectorEngine(
                            column="embedding",
                            embeddings_name="products",
                            dimensions=3,
                            distance="cosine",
                        ),
                    )
                ),
            }
        )
    )
    search = ctx.search.query(
        SearchSpec(
            name="products",
            model_type=ProductRow,
            fields=["id", "title", "product_type", "color"],
        )
    )
    query_vector = [2.0, 5.0, 1.0]
    filters = {"$values": {"product_type": {"$in": ["shirt"]}, "color": "red"}}

    page = await search.search_page(
        "",
        filters=filters,
        pagination={"limit": 3, "offset": 1},
        options={"query_vector": query_vector},
    )

    by_hand = await pgvector_client.fetch_all(
        f"""
        SELECT g.id, -(e.embedding <=> %(qv)s::vector) AS score
        FROM {groups} g
        JOIN {embeddings} e ON e.group_id = g.id
        JOIN {profiles} p ON p.id = g.id
        WHERE g.product_type = ANY(%(types)s) AND p.color = %(color)s
        ORDER BY score DESC, g.id ASC
        LIMIT 3 OFFSET 1
        """,
        {"qv": "[2,5,1]", "types": ["shirt"], "color": "red"},
    )

    assert [hit.id for hit in page.hits] == [row["id"] for row in by_hand]
    assert page.scores == pytest.approx([float(row["score"]) for row in by_hand])
    assert page.count == len(
        await pgvector_client.fetch_all(
            f"SELECT 1 FROM {view} WHERE product_type = 'shirt' AND color = 'red'"
        )
    )

    # The page's data query, the one statement that selects the score.
    ((data_stmt, data_params),) = [
        (stmt, params) for stmt, params in client.fetched if '"_score"' in stmt.as_string()
    ]
    plan = await pgvector_client.fetch_all(
        sql.SQL("EXPLAIN (FORMAT JSON) ") + data_stmt, data_params
    )
    scanned = _relations(plan)

    # The view's three tables, each once: no second copy of the view for a join back.
    assert sorted(scanned) == sorted([groups, embeddings, profiles])
