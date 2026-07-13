"""Shared filter-first ranked pipeline SQL (FTS, vector, PGroonga filter_first)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._engine import RankedPipelineSql
    from ._highlights import HighlightSelect

import attrs
from psycopg import sql

from forze.application.contracts.querying import QueryExpr

from ._pipeline_sql import (
    PipelineAliases,
    build_filtered_cte,
    build_outer_from,
    build_pipeline_with_clause,
    build_scored_cte,
    filtered_select_list,
    outer_join_on_scored,
    scored_join_on_filtered,
)

# ----------------------- #


@attrs.define(frozen=True, slots=True, kw_only=True)
class RankedPipelineParts:
    """Data and optional exact-count SQL fragments for ranked search."""

    with_clause: sql.Composable
    from_outer: sql.Composable
    params_body: list[Any]
    count_with_clause: sql.Composable | None = None
    count_from_outer: sql.Composable | None = None
    count_params: list[Any] | None = None
    candidate_limit: int | None = None


# ....................... #


def build_filter_first_ranked_pipeline(
    *,
    aliases: PipelineAliases,
    join_pairs: Sequence[tuple[str, str]],
    proj_ident: sql.Composable,
    heap_ident: sql.Composable,
    outer_proj_ident: sql.Composable,
    fw: sql.Composable,
    fp: list[Any],
    leg_params: list[Any],
    sw: sql.Composable,
    scored_rank: sql.Composable,
    scored_keys: sql.Composable,
    coalesced: bool,
    heap_fw: sql.Composable | None,
    heap_fp: list[Any],
    cap_kw: dict[str, Any],
    candidate_order_asc: bool = False,
    emit_exact_count_sql: bool = True,
) -> RankedPipelineParts:
    """Build capped data pipeline plus uncapped ``scored`` for exact ``COUNT(*)``."""

    join_vs = outer_join_on_scored(
        join_pairs,
        projection_alias=aliases.projection,
        scored_alias=aliases.scored,
    )
    has_cap = "candidate_limit" in cap_kw and cap_kw.get("candidate_limit") is not None

    if coalesced:
        scored_data = build_scored_cte(
            aliases=aliases,
            scored_keys=scored_keys,
            scored_rank=scored_rank,
            heap_ident=heap_ident,
            join_sf=None,
            sw=sw,
            heap_fw=heap_fw,
            first_in_with=True,
            candidate_order_asc=candidate_order_asc,
            **cap_kw,
        )
        with_clause: sql.Composable = sql.SQL("WITH {}{}").format(
            scored_data,
            sql.SQL(""),
        )
        from_outer: sql.Composable = build_outer_from(
            aliases=aliases,
            proj_ident=outer_proj_ident,
            join_vs=join_vs,
        )
        params_body = [*leg_params, *heap_fp]

        count_with: sql.Composable | None = None
        count_from: sql.Composable | None = None
        count_params: list[Any] | None = None

        if emit_exact_count_sql and has_cap:
            scored_count = build_scored_cte(
                aliases=aliases,
                scored_keys=scored_keys,
                scored_rank=scored_rank,
                heap_ident=heap_ident,
                join_sf=None,
                sw=sw,
                heap_fw=heap_fw,
                first_in_with=True,
                candidate_order_asc=candidate_order_asc,
            )
            count_with = sql.SQL("WITH {}{}").format(scored_count, sql.SQL(""))
            count_from = from_outer
            count_params = list(params_body)

        cap = cap_kw.get("candidate_limit")

        return RankedPipelineParts(
            with_clause=with_clause,
            from_outer=from_outer,
            params_body=params_body,
            count_with_clause=count_with,
            count_from_outer=count_from,
            count_params=count_params,
            candidate_limit=int(cap) if cap is not None else None,
        )

    key_sel = filtered_select_list(
        join_pairs,
        projection_alias=aliases.projection,
    )
    filtered_cte = build_filtered_cte(
        aliases=aliases,
        key_sel=key_sel,
        proj_ident=proj_ident,
        fw=fw,
    )
    join_sf = scored_join_on_filtered(
        join_pairs,
        index_alias=aliases.index,
        filtered_alias=aliases.filtered,
    )
    scored_data = build_scored_cte(
        aliases=aliases,
        scored_keys=scored_keys,
        scored_rank=scored_rank,
        heap_ident=heap_ident,
        join_sf=join_sf,
        sw=sw,
        candidate_order_asc=candidate_order_asc,
        **cap_kw,
    )
    with_clause = build_pipeline_with_clause(filtered_cte, scored_data)
    filtered_from: sql.Composable = build_outer_from(
        aliases=aliases,
        proj_ident=outer_proj_ident,
        join_vs=join_vs,
    )
    params_body = [*fp, *leg_params]

    count_with = None
    count_from = None
    count_params = None

    if emit_exact_count_sql and has_cap:
        scored_count = build_scored_cte(
            aliases=aliases,
            scored_keys=scored_keys,
            scored_rank=scored_rank,
            heap_ident=heap_ident,
            join_sf=join_sf,
            sw=sw,
            candidate_order_asc=candidate_order_asc,
        )
        count_with = build_pipeline_with_clause(filtered_cte, scored_count)
        count_from = filtered_from
        count_params = list(params_body)

    cap = cap_kw.get("candidate_limit")

    return RankedPipelineParts(
        with_clause=with_clause,
        from_outer=filtered_from,
        params_body=params_body,
        count_with_clause=count_with,
        count_from_outer=count_from,
        count_params=count_params,
        candidate_limit=int(cap) if cap is not None else None,
    )


def ranked_parts_to_sql(
    parts: RankedPipelineParts,
    *,
    pipeline: PipelineAliases,
    rank_column: str,
    projection_alias: str,
    browse_count_params: list[Any] | None = None,
    resolved_plan: str | None = None,
    highlight: HighlightSelect | None = None,
) -> RankedPipelineSql:
    """Convert :class:`RankedPipelineParts` to :class:`RankedPipelineSql`."""

    from ._engine import RankedPipelineSql

    count_params = parts.count_params if parts.count_params is not None else browse_count_params

    return RankedPipelineSql(
        with_clause=parts.with_clause,
        from_outer=parts.from_outer,
        params_body=parts.params_body,
        count_params=count_params,
        count_with_clause=parts.count_with_clause,
        count_from_outer=parts.count_from_outer,
        pipeline=pipeline,
        rank_column=rank_column,
        projection_alias=projection_alias,
        resolved_plan=resolved_plan,
        candidate_limit=parts.candidate_limit,
        highlight=highlight,
    )


def coalesced_heap_filter_parts(
    parsed_filters: QueryExpr | None,
    *,
    trivial: bool,
    fw: sql.Composable,
    fp: list[Any],
    heap_fw: sql.Composable | None,
    heap_fp: list[Any],
) -> tuple[sql.Composable | None, list[Any]]:
    """Resolve heap-side filter SQL for coalesced read==heap pipelines."""

    if not trivial and heap_fw is not None:
        return heap_fw, heap_fp

    return None, []
