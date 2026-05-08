"""Constructor validation for search adapters."""

from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.base.errors import CoreError
from forze_postgres.adapters.search import (
    PostgresFTSSearchAdapter,
    PostgresPGroongaSearchAdapter,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName

# ----------------------- #


class _Entity(BaseModel):
    id: UUID
    a: str
    b: str


def _spec() -> SearchSpec[_Entity]:
    return SearchSpec(name="s", model_type=_Entity, fields=["a", "b"])


@pytest.mark.asyncio
async def test_pgroonga_v2_match_combined_empty_string_is_true_predicate() -> None:
    """Empty match text skips PGroonga clause construction (filter-only path uses ``TRUE`` elsewhere)."""
    adapter = PostgresPGroongaSearchAdapter(
        spec=_spec(),
        source_qname=PostgresQualifiedName("public", "v"),
        index_qname=PostgresQualifiedName("public", "i"),
        index_heap_qname=PostgresQualifiedName("public", "h"),
        client=MagicMock(),
        model_type=_Entity,
        introspector=MagicMock(),
        tenant_provider=None,
        tenant_aware=False,
    )
    sw, params = await adapter._pgroonga_match_combined_query("")
    assert params == []
    assert "TRUE" in str(sw)


def test_pgroonga_v2_rejects_duplicate_projection_join_columns() -> None:
    with pytest.raises(CoreError, match="unique"):
        PostgresPGroongaSearchAdapter(
            spec=_spec(),
            source_qname=PostgresQualifiedName("public", "v"),
            index_qname=PostgresQualifiedName("public", "i"),
            index_heap_qname=PostgresQualifiedName("public", "h"),
            client=MagicMock(),
            model_type=_Entity,
            introspector=MagicMock(),
            tenant_provider=None,
            tenant_aware=False,
            join_pairs=[("id", "c1"), ("id", "c2")],
        )


def test_fts_v2_rejects_duplicate_projection_join_columns() -> None:
    with pytest.raises(CoreError, match="unique"):
        PostgresFTSSearchAdapter(
            spec=_spec(),
            index_qname=PostgresQualifiedName("public", "i"),
            source_qname=PostgresQualifiedName("public", "v"),
            index_heap_qname=PostgresQualifiedName("public", "h"),
            fts_groups={"A": ("a",), "B": ("b",)},
            client=MagicMock(),
            model_type=_Entity,
            introspector=MagicMock(),
            tenant_provider=None,
            tenant_aware=False,
            join_pairs=[("id", "c1"), ("id", "c2")],
        )
