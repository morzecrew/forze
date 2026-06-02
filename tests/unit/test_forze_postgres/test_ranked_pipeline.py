"""Unit tests for :mod:`forze_postgres.adapters.search._ranked_pipeline`."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg")

from psycopg import sql

from forze_postgres.adapters.search._pipeline_sql import (
    PipelineAliases,
    scored_key_columns,
    scored_order_by_rank_alias,
)
from forze_postgres.adapters.search._ranked_pipeline import (
    build_filter_first_ranked_pipeline,
    ranked_parts_to_sql,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName

_JOIN = (("id", "id"),)
_ALIASES = PipelineAliases(rank_column="_fts_rank")


def test_filter_first_emits_uncapped_count_sql_when_capped() -> None:
    keys = scored_key_columns(_JOIN, index_alias=_ALIASES.index)
    rank = sql.SQL("(0)::double precision AS {}").format(
        sql.Identifier(_ALIASES.rank_column),
    )
    cap_kw = {
        "candidate_limit": 5,
        "scored_order": scored_order_by_rank_alias(_ALIASES.rank_column),
    }

    parts = build_filter_first_ranked_pipeline(
        aliases=_ALIASES,
        join_pairs=_JOIN,
        proj_ident=PostgresQualifiedName("public", "docs_v").ident(),
        heap_ident=PostgresQualifiedName("public", "docs_h").ident(),
        outer_proj_ident=PostgresQualifiedName("public", "docs_v").ident(),
        fw=sql.SQL("TRUE"),
        fp=[],
        leg_params=[],
        sw=sql.SQL("TRUE"),
        scored_rank=rank,
        scored_keys=keys,
        coalesced=False,
        heap_fw=None,
        heap_fp=[],
        cap_kw=cap_kw,
        emit_exact_count_sql=True,
    )

    data_with = parts.with_clause.as_string()
    count_with = parts.count_with_clause.as_string() if parts.count_with_clause else ""

    assert "LIMIT 5" in data_with
    assert "LIMIT" not in count_with or count_with.count("LIMIT") == 0
    assert parts.count_from_outer is not None
    assert parts.count_params is not None

    pipeline = ranked_parts_to_sql(
        parts,
        pipeline=_ALIASES,
        rank_column=_ALIASES.rank_column,
        projection_alias="v",
    )
    assert pipeline.count_with_clause is not None
    assert pipeline.count_from_outer is not None


def test_coalesced_with_heap_filter_skips_filtered_cte() -> None:
    keys = scored_key_columns(_JOIN, index_alias=_ALIASES.index)
    rank = sql.SQL("(0)::double precision AS {}").format(
        sql.Identifier(_ALIASES.rank_column),
    )

    parts = build_filter_first_ranked_pipeline(
        aliases=_ALIASES,
        join_pairs=_JOIN,
        proj_ident=PostgresQualifiedName("public", "docs_v").ident(),
        heap_ident=PostgresQualifiedName("public", "docs_h").ident(),
        outer_proj_ident=PostgresQualifiedName("public", "docs_h").ident(),
        fw=sql.SQL("TRUE"),
        fp=[],
        leg_params=[],
        sw=sql.SQL("TRUE"),
        scored_rank=rank,
        scored_keys=keys,
        coalesced=True,
        heap_fw=sql.SQL("{}.id = %s").format(sql.Identifier(_ALIASES.index)),
        heap_fp=["x"],
        cap_kw={},
        emit_exact_count_sql=False,
    )

    text = parts.with_clause.as_string()
    assert ', "f" AS (' not in text
