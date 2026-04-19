"""Unit tests for shared FTS / PGroonga SQL helpers and hub leg dispatch."""

from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.base.errors import CoreError
from forze_postgres.adapters.search._fts_sql import (
    fts_effective_group_weights,
    fts_rank_cd_weight_array,
)
from forze_postgres.adapters.search._pgroonga_sql import pgroonga_heap_column_names
from forze_postgres.adapters.search._pgroonga_sql import pgroonga_match_clause
from forze_postgres.adapters.search.hub import (
    FtsHubLegEngine,
    HubLegRuntime,
    PgroongaHubLegEngine,
    hub_leg_engine_for,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName


class _Doc(BaseModel):
    a: str
    b: str


class _TitleContent(BaseModel):
    title: str
    content: str


def _spec(*, fields: tuple[str, ...] = ("a", "b")) -> SearchSpec[_Doc]:
    return SearchSpec(name="t", model_type=_Doc, fields=list(fields))


def test_pgroonga_heap_column_names_without_map() -> None:
    assert pgroonga_heap_column_names(_spec(), None) == ["a", "b"]


def test_pgroonga_heap_column_names_with_field_map() -> None:
    assert pgroonga_heap_column_names(
        _spec(),
        {"a": "col_a", "b": "col_b"},
    ) == ["col_a", "col_b"]


def test_fts_rank_cd_weight_array_d_c_b_a_order() -> None:
    gw = {"A": 0.1, "B": 0.2, "C": 0.3, "D": 0.4}
    assert fts_rank_cd_weight_array(gw) == [0.4, 0.3, 0.2, 0.1]


def test_fts_effective_group_weights_respects_options_weights() -> None:
    spec = _spec(fields=("a", "b"))
    groups = {"A": ("a",), "B": ("b",)}
    out = fts_effective_group_weights(
        spec,
        groups,
        {"weights": {"a": 1.0, "b": 0.5}},
    )
    assert out["A"] > 0 and out["B"] > 0


def test_hub_leg_engine_for_pgroonga() -> None:
    leg = HubLegRuntime(
        search=_spec(),
        index_qname=PostgresQualifiedName("public", "idx"),
        index_heap_qname=PostgresQualifiedName("public", "heap"),
        hub_fk_column="fk",
        heap_pk_column="id",
        engine="pgroonga",
    )
    assert isinstance(hub_leg_engine_for(leg), PgroongaHubLegEngine)


def test_hub_leg_engine_for_fts() -> None:
    leg = HubLegRuntime(
        search=_spec(),
        index_qname=PostgresQualifiedName("public", "idx"),
        index_heap_qname=PostgresQualifiedName("public", "heap"),
        hub_fk_column="fk",
        heap_pk_column="id",
        engine="fts",
        fts_groups={"A": ("a",), "B": ("b",)},
    )
    assert isinstance(hub_leg_engine_for(leg), FtsHubLegEngine)


def test_hub_leg_engine_for_rejects_unknown_engine() -> None:
    leg = HubLegRuntime(
        search=_spec(),
        index_qname=PostgresQualifiedName("public", "idx"),
        index_heap_qname=PostgresQualifiedName("public", "heap"),
        hub_fk_column="fk",
        heap_pk_column="id",
        engine=cast(Any, "vector"),
    )
    with pytest.raises(CoreError, match="Unsupported hub search leg engine"):
        hub_leg_engine_for(leg)


@pytest.mark.asyncio
async def test_fts_hub_leg_engine_requires_fts_groups() -> None:
    leg = HubLegRuntime(
        search=_spec(),
        index_qname=PostgresQualifiedName("public", "idx"),
        index_heap_qname=PostgresQualifiedName("public", "heap"),
        hub_fk_column="fk",
        heap_pk_column="id",
        engine="fts",
        fts_groups=None,
    )
    eng = FtsHubLegEngine()
    with pytest.raises(CoreError, match="FTS hub leg requires fts_groups"):
        await eng.build_leg(
            leg,
            introspector=MagicMock(),
            index_alias="t",
            query="q",
            options=None,
            score_column="s",
        )


@pytest.mark.asyncio
async def test_pgroonga_match_non_array_index_uses_first_heap_column() -> None:
    idx = PostgresQualifiedName("public", "idx_pg")
    info = MagicMock()
    info.expr = "(title)"
    introspector = MagicMock()
    introspector.get_index_info = AsyncMock(return_value=info)

    spec = SearchSpec(name="s", model_type=_TitleContent, fields=["title", "content"])
    sw, params = await pgroonga_match_clause(
        search=spec,
        index_field_map=None,
        index_qname=idx,
        introspector=introspector,
        index_alias="t",
        query="hello",
        options=None,
    )
    assert "ARRAY" not in str(sw)
    assert params[0] == "hello"


@pytest.mark.asyncio
async def test_pgroonga_match_non_array_fuzzy_uses_spec_ratio() -> None:
    idx = PostgresQualifiedName("public", "idx_pg")
    info = MagicMock()
    info.expr = "(body)"
    introspector = MagicMock()
    introspector.get_index_info = AsyncMock(return_value=info)

    spec = SearchSpec(
        name="s",
        model_type=_Doc,
        fields=["a", "b"],
        fuzzy={"max_distance_ratio": 0.11},
    )
    sw, params = await pgroonga_match_clause(
        search=spec,
        index_field_map={"a": "body", "b": "other"},
        index_qname=idx,
        introspector=introspector,
        index_alias="t",
        query=" x ",
        options={"fuzzy": True},
    )
    assert "fuzzy_max_distance_ratio" in str(sw)
    assert 0.11 in params


@pytest.mark.asyncio
async def test_pgroonga_match_array_fuzzy_includes_ratio() -> None:
    idx = PostgresQualifiedName("public", "idx_arr")
    info = MagicMock()
    info.expr = "(ARRAY[a, b])"
    introspector = MagicMock()
    introspector.get_index_info = AsyncMock(return_value=info)

    spec = _spec()
    sw, params = await pgroonga_match_clause(
        search=spec,
        index_field_map=None,
        index_qname=idx,
        introspector=introspector,
        index_alias="t",
        query="z",
        options={"fuzzy": True},
    )
    assert "fuzzy_max_distance_ratio" in str(sw)
    assert isinstance(params[-1], float)
