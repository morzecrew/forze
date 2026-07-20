"""Constructor validation for search adapters."""

from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException
from forze_postgres.adapters.search import (
    PostgresFTSSearchAdapter,
    PostgresPGroongaSearchAdapter,
)
from forze_postgres.adapters.search._leg_pgroonga import build_pgroonga_leg

# ----------------------- #


class _Entity(BaseModel):
    id: UUID
    a: str
    b: str


def _spec() -> SearchSpec[_Entity]:
    return SearchSpec(name="s", model_type=_Entity, fields=["a", "b"])


class _EntityWithExtra(BaseModel):
    id: UUID
    a: str
    note: str = ""  # returned, not indexed — eligible for leniency


def test_search_adapter_excludes_lenient_read_fields_from_projection() -> None:
    # A lenient read field has no column, so the adapter must not project it
    # (read_fields drives the result projection and cursor keyset).
    spec = SearchSpec(
        name="s",
        model_type=_EntityWithExtra,
        fields=["a"],
        lenient_read_fields={"note"},
    )
    adapter = PostgresPGroongaSearchAdapter(
        spec=spec,
        codec=spec.resolved_read_codec,
        relation=("public", "v"),
        index_relation=("public", "i"),
        index_heap_relation=("public", "h"),
        client=MagicMock(),
        model_type=_EntityWithExtra,
        introspector=MagicMock(),
        tenant_provider=None,
        tenant_aware=False,
        lenient_read_fields=spec.lenient_read_fields,
    )

    assert adapter.lenient_read_fields == frozenset({"note"})
    assert "note" not in adapter.read_fields
    assert {"id", "a"} <= adapter.read_fields


@pytest.mark.asyncio
async def test_pgroonga_v2_match_combined_empty_string_is_true_predicate() -> None:
    """Empty match text skips PGroonga clause construction (filter-only path uses ``TRUE`` elsewhere)."""
    spec = _spec()
    adapter = PostgresPGroongaSearchAdapter(
        spec=spec,
        codec=spec.resolved_read_codec,
        relation=("public", "v"),
        index_relation=("public", "i"),
        index_heap_relation=("public", "h"),
        client=MagicMock(),
        model_type=_Entity,
        introspector=MagicMock(),
        tenant_provider=None,
        tenant_aware=False,
    )
    sw, _rank, params = await build_pgroonga_leg(
        introspector=adapter.introspector,
        index_qname=await adapter._index_qname(),
        search=adapter.spec,
        index_field_map=adapter.index_field_map,
        index_alias="t",
        queries=(),
        options=None,
        score_column="_pgroonga_rank",
        pgroonga_score_version=adapter.pgroonga_score_version,
    )
    assert params == []
    assert "TRUE" in str(sw)


def test_pgroonga_v2_rejects_duplicate_projection_join_columns() -> None:
    spec = _spec()
    with pytest.raises(CoreException, match="unique"):
        PostgresPGroongaSearchAdapter(
            spec=spec,
            codec=spec.resolved_read_codec,
            relation=("public", "v"),
            index_relation=("public", "i"),
            index_heap_relation=("public", "h"),
            client=MagicMock(),
            model_type=_Entity,
            introspector=MagicMock(),
            tenant_provider=None,
            tenant_aware=False,
            join_pairs=[("id", "c1"), ("id", "c2")],
        )


def test_fts_v2_rejects_duplicate_projection_join_columns() -> None:
    spec = _spec()
    with pytest.raises(CoreException, match="unique"):
        PostgresFTSSearchAdapter(
            spec=spec,
            codec=spec.resolved_read_codec,
            index_relation=("public", "i"),
            relation=("public", "v"),
            index_heap_relation=("public", "h"),
            fts_groups={"A": ("a",), "B": ("b",)},
            client=MagicMock(),
            model_type=_Entity,
            introspector=MagicMock(),
            tenant_provider=None,
            tenant_aware=False,
            join_pairs=[("id", "c1"), ("id", "c2")],
        )
