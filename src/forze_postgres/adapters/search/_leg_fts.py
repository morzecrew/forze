"""Shared FTS leg scoring for simple adapters and hub legs."""

from __future__ import annotations

from typing import Any, Sequence

from psycopg import sql

from forze.application.contracts.search import SearchOptions, SearchSpec, effective_phrase_combine

from ...kernel.gateways import PostgresQualifiedName
from ...kernel.catalog.introspect import PostgresIntrospector
from ._fts_sql import (
    FtsGroupLetter,
    fts_effective_group_weights,
    fts_match_predicate,
    fts_rank_cd_expr,
    fts_rank_cd_weight_array,
    fts_resolve_tsvector_expr,
    fts_tsquery_expr,
    fts_tsquery_expr_conjunction,
    fts_tsquery_expr_disjunction,
)

# ----------------------- #


async def build_fts_leg(
    *,
    introspector: PostgresIntrospector,
    index_qname: PostgresQualifiedName,
    search: SearchSpec[Any],
    fts_groups: dict[FtsGroupLetter, Sequence[str]],
    index_alias: str,
    queries: tuple[str, ...],
    options: SearchOptions | None,
    score_column: str,
) -> tuple[sql.Composable, sql.Composable, list[Any]]:
    """Build heap ``WHERE``, rank ``SELECT`` fragment, and leg parameters.

    Parameter order (non-empty query): ``weights`` (``float4[]``), rank ``tsquery``,
    then where ``tsquery`` (each from ``fts_tsquery_expr``; may duplicate placeholders).

    Empty ``queries``: ``TRUE``, zero rank, no parameters.
    """

    _ = index_alias

    if not queries:
        return (
            sql.SQL("TRUE"),
            sql.SQL("(0)::double precision AS {}").format(
                sql.Identifier(score_column),
            ),
            [],
        )

    tsv = await fts_resolve_tsvector_expr(introspector, index_qname)

    if len(queries) == 1:
        tsw_where, tsp_w = fts_tsquery_expr(queries[0], options=options)
        tsw_rank, tsp_r = fts_tsquery_expr(queries[0], options=options)

    else:
        fn = (
            fts_tsquery_expr_disjunction
            if effective_phrase_combine(options) == "any"
            else fts_tsquery_expr_conjunction
        )
        tsw_where, tsp_w = fn(queries, options=options)
        tsw_rank, tsp_r = fn(queries, options=options)

    sw = fts_match_predicate(tsv=tsv, tsw=tsw_where)
    gw = fts_effective_group_weights(search, fts_groups, options)
    fts_weights = fts_rank_cd_weight_array(gw)
    rank_expr = sql.SQL("{} AS {}").format(
        fts_rank_cd_expr(tsv=tsv, tsw=tsw_rank),
        sql.Identifier(score_column),
    )
    leg_params: list[Any] = [fts_weights, *tsp_r, *tsp_w]

    return sw, rank_expr, leg_params
