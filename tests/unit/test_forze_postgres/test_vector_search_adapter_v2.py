"""Unit tests for :class:`PostgresVectorSearchAdapterV2` (no database)."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.contracts.embeddings import EmbeddingsSpec
from forze.application.contracts.search import SearchSpec
from forze.base.errors import CoreError
from forze_postgres.adapters.search import PostgresVectorSearchAdapterV2
from forze_postgres.kernel.gateways import PostgresQualifiedName


class _M(BaseModel):
    id: UUID
    label: str


def _minimal_vector_port(
    *,
    join_pairs: tuple[tuple[str, str], ...] | None = None,
) -> PostgresVectorSearchAdapterV2[_M]:
    return PostgresVectorSearchAdapterV2(
        spec=SearchSpec(name="t", model_type=_M, fields=["id", "label"]),
        index_qname=PostgresQualifiedName("public", "idx"),
        source_qname=PostgresQualifiedName("public", "heap"),
        index_heap_qname=PostgresQualifiedName("public", "heap"),
        embedder=MagicMock(),
        embeddings_spec=EmbeddingsSpec(name="e", dimensions=3),
        vector_column="emb",
        client=MagicMock(),
        model_type=_M,
        introspector=MagicMock(),
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
async def test_search_with_cursor_is_not_implemented() -> None:
    port = _minimal_vector_port()
    with pytest.raises(CoreError, match="search_with_cursor is not implemented"):
        await port.search_with_cursor("q")
