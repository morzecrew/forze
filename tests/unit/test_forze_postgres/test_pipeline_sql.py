"""Unit tests for :mod:`forze_postgres.adapters.search._pipeline_sql`."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg")

from psycopg import sql

from forze.base.errors import CoreError
from forze_postgres.adapters.search._pipeline_sql import (
    PipelineAliases,
    build_filtered_cte,
    build_outer_from,
    build_pipeline_with_clause,
    build_rank_first_order,
    build_scored_cte,
    filtered_select_list,
    outer_join_on_scored,
    scored_join_on_filtered,
    scored_key_columns,
    validate_join_pairs,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName

_JOIN = (("id", "id"), ("tenant_id", "tid"))
_ALIASES = PipelineAliases(rank_column="_fts_rank")


def test_validate_join_pairs_rejects_duplicate_projection_keys() -> None:
    with pytest.raises(CoreError, match="unique projection"):
        validate_join_pairs((("id", "a"), ("id", "b")))


def test_filtered_select_list_renders() -> None:
    frag = filtered_select_list(_JOIN, projection_alias="v")
    assert "v" in frag.as_string()
    assert "id" in frag.as_string()
    assert "tenant_id" in frag.as_string()


def test_scored_join_on_filtered_renders() -> None:
    frag = scored_join_on_filtered(_JOIN, index_alias="t", filtered_alias="f")
    text = frag.as_string()
    assert "t" in text and "f" in text
    assert "tid" in text


def test_outer_join_on_scored_renders() -> None:
    frag = outer_join_on_scored(_JOIN, projection_alias="v", scored_alias="s")
    text = frag.as_string()
    assert "v" in text and "s" in text


def test_scored_key_columns_renders() -> None:
    frag = scored_key_columns(_JOIN, index_alias="t")
    assert "AS" in frag.as_string()


def test_build_filtered_cte_renders() -> None:
    key_sel = filtered_select_list(_JOIN, projection_alias=_ALIASES.projection)
    cte = build_filtered_cte(
        aliases=_ALIASES,
        key_sel=key_sel,
        proj_ident=PostgresQualifiedName("public", "docs_v").ident(),
        fw=sql.SQL("TRUE"),
    )
    text = cte.as_string()
    assert "f" in text
    assert "docs_v" in text


def test_build_scored_cte_renders() -> None:
    keys = scored_key_columns(_JOIN, index_alias=_ALIASES.index)
    rank = sql.SQL("(0)::double precision AS {}").format(
        sql.Identifier(_ALIASES.rank_column),
    )
    cte = build_scored_cte(
        aliases=_ALIASES,
        scored_keys=keys,
        scored_rank=rank,
        heap_ident=PostgresQualifiedName("public", "docs_h").ident(),
        join_sf=scored_join_on_filtered(
            _JOIN,
            index_alias=_ALIASES.index,
            filtered_alias=_ALIASES.filtered,
        ),
        sw=sql.SQL("TRUE"),
    )
    text = cte.as_string()
    assert "s" in text
    assert "docs_h" in text


def test_build_outer_from_and_rank_order() -> None:
    join_vs = outer_join_on_scored(
        _JOIN,
        projection_alias=_ALIASES.projection,
        scored_alias=_ALIASES.scored,
    )
    outer = build_outer_from(
        aliases=_ALIASES,
        proj_ident=PostgresQualifiedName("public", "docs_v").ident(),
        join_vs=join_vs,
    )
    order = build_rank_first_order(aliases=_ALIASES, extra_order=None)
    assert "DESC NULLS LAST" in order.as_string()
    assert "_fts_rank" in order.as_string()
    assert "INNER JOIN" in outer.as_string()


def test_build_pipeline_with_clause() -> None:
    filt = build_filtered_cte(
        aliases=_ALIASES,
        key_sel=filtered_select_list(_JOIN, projection_alias="v"),
        proj_ident=sql.SQL("public.v"),
        fw=sql.SQL("TRUE"),
    )
    scored = build_scored_cte(
        aliases=_ALIASES,
        scored_keys=scored_key_columns(_JOIN, index_alias="t"),
        scored_rank=sql.SQL("0"),
        heap_ident=sql.SQL("public.h"),
        join_sf=sql.SQL("TRUE"),
        sw=sql.SQL("TRUE"),
    )
    wc = build_pipeline_with_clause(filt, scored)
    assert wc.as_string().startswith("WITH ")
