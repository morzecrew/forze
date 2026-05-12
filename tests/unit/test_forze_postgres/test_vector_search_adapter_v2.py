"""Unit tests for :class:`PostgresVectorSearchAdapterV2` (no database)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.contracts.embeddings import EmbeddingsSpec
from forze.application.contracts.search import SearchSpec
from forze.base.errors import CoreError
from forze_postgres.adapters.search import PostgresVectorSearchAdapter
from forze_postgres.adapters.search._vector_sql import vector_param_literal
from forze_postgres.kernel.gateways import PostgresQualifiedName


class _M(BaseModel):
    id: UUID
    label: str


def _minimal_vector_port(
    *,
    join_pairs: tuple[tuple[str, str], ...] | None = None,
) -> PostgresVectorSearchAdapter[_M]:
    intro = MagicMock()
    intro.get_column_types = AsyncMock(return_value={})
    return PostgresVectorSearchAdapter(
        spec=SearchSpec(name="t", model_type=_M, fields=["id", "label"]),
        index_qname=PostgresQualifiedName("public", "idx"),
        source_qname=PostgresQualifiedName("public", "heap"),
        index_heap_qname=PostgresQualifiedName("public", "heap"),
        embedder=MagicMock(),
        embeddings_spec=EmbeddingsSpec(name="e", dimensions=3),
        vector_column="emb",
        client=MagicMock(),
        model_type=_M,
        introspector=intro,
        tenant_aware=False,
        tenant_provider=None,
        filter_table_alias="v",
        join_pairs=join_pairs,
    )


def test_join_pairs_must_use_unique_projection_columns() -> None:
    with pytest.raises(CoreError, match="unique projection"):
        _minimal_vector_port(
            join_pairs=(("id", "a"), ("id", "b")),
        )


@pytest.mark.asyncio
async def test_search_with_cursor_browse_calls_fetch_all() -> None:
    uid = UUID("00000000-0000-4000-8000-000000000001")
    port = _minimal_vector_port()
    port.client.fetch_all = AsyncMock(
        return_value=[{"id": uid, "label": "only"}],
    )
    page = await port.search_cursor("", cursor={"limit": 5})
    assert len(page.hits) == 1
    assert page.hits[0].id == uid
    port.client.fetch_all.assert_awaited_once()


@pytest.mark.asyncio
async def test_search_with_cursor_ranked_calls_embed_and_fetch() -> None:
    uid = UUID("00000000-0000-4000-8000-000000000002")
    port = _minimal_vector_port()
    qvec = [0.1, 0.2, 0.3]
    port.embedder.embed_one = AsyncMock(return_value=qvec)
    port.client.fetch_all = AsyncMock(
        return_value=[
            {
                "id": uid,
                "label": "hit",
                "_vector_rank": 0.42,
            },
        ],
    )
    page = await port.project_search_cursor(
        ["id", "label"],
        "q",
        cursor={"limit": 3},
    )
    assert len(page.hits) == 1
    assert set(page.hits[0].keys()) == {"id", "label"}
    port.embedder.embed_one.assert_awaited_once()
    port.client.fetch_all.assert_awaited_once()
    call = port.client.fetch_all.call_args
    params = call[0][1]
    assert vector_param_literal(qvec) in params
