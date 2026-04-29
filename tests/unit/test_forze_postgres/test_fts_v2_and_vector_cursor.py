"""Cursor validation and error paths for FTS and vector v2 search adapters (no database)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.contracts.embeddings import EmbeddingsSpec
from forze.application.contracts.search import SearchSpec
from forze.base.errors import CoreError
from forze_postgres.adapters.search import (
    PostgresFTSSearchAdapterV2,
    PostgresVectorSearchAdapterV2,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName


class _M(BaseModel):
    id: UUID
    label: str


def _fts() -> PostgresFTSSearchAdapterV2[_M]:
    intro = MagicMock()
    intro.get_column_types = AsyncMock(return_value={})
    return PostgresFTSSearchAdapterV2(
        spec=SearchSpec(name="t", model_type=_M, fields=["id", "label"]),
        index_qname=PostgresQualifiedName("public", "idx"),
        source_qname=PostgresQualifiedName("public", "v"),
        index_heap_qname=PostgresQualifiedName("public", "h"),
        fts_groups={"A": ("label",)},
        client=MagicMock(),
        model_type=_M,
        introspector=intro,
        tenant_aware=False,
        tenant_provider=None,
        filter_table_alias="v",
    )


def _vec() -> PostgresVectorSearchAdapterV2[_M]:
    intro = MagicMock()
    intro.get_column_types = AsyncMock(return_value={})
    return PostgresVectorSearchAdapterV2(
        spec=SearchSpec(name="t", model_type=_M, fields=["id", "label"]),
        index_qname=PostgresQualifiedName("public", "idx"),
        source_qname=PostgresQualifiedName("public", "h"),
        index_heap_qname=PostgresQualifiedName("public", "h"),
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


@pytest.mark.asyncio
async def test_fts_v2_search_with_cursor_rejects_after_and_before() -> None:
    p = _fts()
    with pytest.raises(CoreError, match="at most one"):
        await p.search_with_cursor("q", cursor={"after": "a", "before": "b"})


@pytest.mark.asyncio
async def test_fts_v2_search_with_cursor_rejects_non_positive_limit() -> None:
    p = _fts()
    with pytest.raises(CoreError, match="positive"):
        await p.search_with_cursor("q", cursor={"limit": 0})


@pytest.mark.asyncio
async def test_vector_v2_search_with_cursor_rejects_after_and_before() -> None:
    p = _vec()
    with pytest.raises(CoreError, match="at most one"):
        await p.search_with_cursor("q", cursor={"after": "a", "before": "b"})


@pytest.mark.asyncio
async def test_vector_v2_search_with_cursor_rejects_non_positive_limit() -> None:
    p = _vec()
    with pytest.raises(CoreError, match="positive"):
        await p.search_with_cursor("q", cursor={"limit": 0})
