"""A ranked offset search over one relation reads it once and caps its candidates at the page.

When the read relation is the index heap, the ranked CTE can carry the read model's columns
itself, so the outer query reads the CTE instead of joining the relation back by id: a view
there is expanded and planned once, not twice. With no snapshot to write, the CTE keeps only
the rows the page can reach.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from psycopg import sql
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.contracts.embeddings import EmbeddingsSpec
from forze.application.contracts.search import SearchSpec
from forze_postgres.adapters.search import PostgresFTSSearchAdapter, PostgresVectorSearchAdapter

pytestmark = pytest.mark.unit

QV = [0.5, 0.25, 0.125]


class _M(BaseModel):
    id: UUID
    label: str


def _port(*, read: str = "heap") -> PostgresVectorSearchAdapter[_M]:
    intro = MagicMock()
    intro.get_column_types = AsyncMock(return_value={})
    spec = SearchSpec(name="t", model_type=_M, fields=["id", "label"])
    port = PostgresVectorSearchAdapter(
        spec=spec,
        codec=spec.resolved_read_codec,
        index_relation=("public", "idx"),
        relation=("public", read),
        index_heap_relation=("public", "heap"),
        embedder=MagicMock(),
        embeddings_spec=EmbeddingsSpec(name="e", dimensions=3),
        vector_column="emb",
        client=MagicMock(),
        model_type=_M,
        introspector=intro,
        tenant_aware=False,
        tenant_provider=None,
        filter_table_alias="v",
    )
    port.client.fetch_all = AsyncMock(return_value=[])
    port.client.fetch_value = AsyncMock(return_value=0)
    port.client.fetch_one = AsyncMock(return_value={"count": 0})
    return port


def _statements(port: Any) -> list[str]:
    return [call[0][0].as_string() for call in port.client.fetch_all.call_args_list]


class TestOneRelation:
    async def test_the_data_query_names_the_relation_once(self) -> None:
        port = _port()

        await port.search("", pagination={"limit": 3}, options={"query_vector": QV})

        (stmt,) = _statements(port)
        assert stmt.count('"public"."heap"') == 1
        assert "JOIN" not in stmt
        # The page's columns and score come from the ranked CTE.
        assert 'FROM "s" "v"' in stmt
        assert '"v"."_vector_rank" AS "_score"' in stmt

    async def test_a_distinct_read_relation_still_joins_back(self) -> None:
        port = _port(read="heap_view")

        await port.search("", pagination={"limit": 3}, options={"query_vector": QV})

        stmts = _statements(port)
        assert any("JOIN" in stmt for stmt in stmts)

    async def test_a_cursor_walk_keeps_the_join(self) -> None:
        port = _port()

        await port.search_cursor("", cursor={"limit": 3}, options={"query_vector": QV})

        (stmt,) = _statements(port)
        assert "INNER JOIN" in stmt


def _fts(*, highlightable: bool = False) -> PostgresFTSSearchAdapter[_M]:
    intro = MagicMock()
    intro.get_column_types = AsyncMock(return_value={})
    intro.get_index_info = AsyncMock(return_value=MagicMock(expr="to_tsvector('simple', label)"))
    spec = SearchSpec(
        name="t",
        model_type=_M,
        fields=["id", "label"],
        highlightable_fields=frozenset({"label"}) if highlightable else None,
    )
    port = PostgresFTSSearchAdapter(
        spec=spec,
        codec=spec.resolved_read_codec,
        index_relation=("public", "idx"),
        relation=("public", "heap"),
        index_heap_relation=("public", "heap"),
        fts_groups={"A": ("label",)},
        client=MagicMock(),
        model_type=_M,
        introspector=intro,
        tenant_aware=False,
        tenant_provider=None,
        filter_table_alias="v",
    )
    port.client.fetch_all = AsyncMock(return_value=[])
    port.client.fetch_value = AsyncMock(return_value=5)
    return port


class TestTextEngines:
    async def test_an_fts_page_reads_the_relation_once(self) -> None:
        port = _fts()

        await port.search("blue", pagination={"limit": 3})

        (stmt,) = _statements(port)
        assert stmt.count('"public"."heap"') == 1
        assert "JOIN" not in stmt

    async def test_a_highlighted_page_keeps_the_join(self) -> None:
        port = _fts(highlightable=True)

        await port.search("blue", pagination={"limit": 3}, options={"highlight": True})

        (stmt,) = _statements(port)
        assert "INNER JOIN" in stmt


class TestPageCap:
    async def test_the_ranked_cte_keeps_only_the_rows_the_page_reaches(self) -> None:
        port = _fts()

        await port.search("blue", pagination={"limit": 3, "offset": 6})

        (stmt,) = _statements(port)
        assert "LIMIT 9" in stmt
        assert "LIMIT 5000" not in stmt

    async def test_an_approximate_total_keeps_the_configured_cap(self) -> None:
        port = _fts()

        await port.search_page(
            "blue", pagination={"limit": 3}, options={"search_count": "approximate"}
        )

        # The approximate total counts the capped pool, so the pool stays the configured cap.
        assert all("LIMIT 3 " not in stmt for stmt in _statements(port))
        counted = port.client.fetch_value.call_args[0][0].as_string()
        assert "LIMIT 5000" in counted

    async def test_a_vector_page_keeps_its_top_k_cap(self) -> None:
        port = _port()

        await port.search("", pagination={"limit": 3}, options={"query_vector": QV})

        (stmt,) = _statements(port)
        assert "LIMIT 5000" in stmt

    async def test_the_fingerprint_keeps_the_configured_cap(self) -> None:
        port = _fts()
        pipeline = await port._build_ranked_pipeline_sql(  # pyright: ignore[reportPrivateUsage]
            query="blue",
            filters=None,
            options=None,
            fw=sql.SQL("TRUE"),
            fp=[],
            terms=("blue",),
            pagination={"limit": 3},
        )

        # A snapshot written for page one must still match page two's fingerprint.
        assert pipeline.candidate_limit == 5000
