"""Shared projection + index-heap CTE SQL for Postgres simple search adapters."""

from __future__ import annotations

from typing import Sequence

import attrs
from psycopg import sql

from forze.base.exceptions import exc

# ----------------------- #


@attrs.define(frozen=True, slots=True, kw_only=True)
class PipelineAliases:
    """CTE and table aliases for the filtered → scored → projection join pipeline."""

    rank_column: str
    """Rank column name inside the scored CTE (e.g. ``_fts_rank``)."""
    filtered: str = "f"
    index: str = "t"
    projection: str = "v"
    scored: str = "s"


# ....................... #


def scored_order_by_rank_alias(rank_column: str) -> sql.Composable:
    """``ORDER BY`` target for capped ``scored`` CTEs (output alias, not a heap column)."""

    return sql.Identifier(rank_column)


# ....................... #


def validate_join_pairs(join_pairs: Sequence[tuple[str, str]]) -> None:
    """Require unique projection-side column names in ``join_pairs``."""

    proj_keys = {pc for pc, _ in join_pairs}

    if len(proj_keys) != len(join_pairs):
        raise exc.internal("join_pairs must use unique projection column names.")


# ....................... #


def filtered_select_list(
    join_pairs: Sequence[tuple[str, str]],
    *,
    projection_alias: str,
) -> sql.Composable:
    """``SELECT v.pc AS pc`` list for the filtered CTE."""

    parts = [
        sql.SQL("{} AS {}").format(
            sql.Identifier(projection_alias, pc),
            sql.Identifier(pc),
        )
        for pc, _ in join_pairs
    ]

    return sql.SQL(", ").join(parts)


# ....................... #


def scored_join_on_filtered(
    join_pairs: Sequence[tuple[str, str]],
    *,
    index_alias: str,
    filtered_alias: str,
) -> sql.Composable:
    """``t.ic = f.pc`` predicates joining index heap to filtered keys."""

    parts = [
        sql.SQL("{} = {}").format(
            sql.Identifier(index_alias, ic),
            sql.Identifier(filtered_alias, pc),
        )
        for pc, ic in join_pairs
    ]

    return sql.SQL(" AND ").join(parts)


# ....................... #


def outer_join_on_scored(
    join_pairs: Sequence[tuple[str, str]],
    *,
    projection_alias: str,
    scored_alias: str,
) -> sql.Composable:
    """``v.pc = s.pc`` predicates joining projection to scored keys."""

    parts = [
        sql.SQL("{} = {}").format(
            sql.Identifier(projection_alias, pc),
            sql.Identifier(scored_alias, pc),
        )
        for pc, _ in join_pairs
    ]

    return sql.SQL(" AND ").join(parts)


# ....................... #


def scored_key_columns(
    join_pairs: Sequence[tuple[str, str]],
    *,
    index_alias: str,
) -> sql.Composable:
    """``t.ic AS pc`` column list for the scored CTE."""

    return sql.SQL(", ").join(
        sql.SQL("{} AS {}").format(
            sql.Identifier(index_alias, ic),
            sql.Identifier(pc),
        )
        for pc, ic in join_pairs
    )


# ....................... #


def build_filtered_cte(
    *,
    aliases: PipelineAliases,
    key_sel: sql.Composable,
    proj_ident: sql.Composable,
    fw: sql.Composable,
) -> sql.Composable:
    """``filtered AS (SELECT keys FROM projection WHERE filters)``."""

    return sql.SQL(
        """
            {filtered} AS (
                SELECT {key_sel}
                FROM {proj} {pa}
                WHERE {fw}
            )"""
    ).format(
        filtered=sql.Identifier(aliases.filtered),
        key_sel=key_sel,
        proj=proj_ident,
        pa=sql.Identifier(aliases.projection),
        fw=fw,
    )


# ....................... #


def build_scored_cte(
    *,
    aliases: PipelineAliases,
    scored_keys: sql.Composable,
    scored_rank: sql.Composable,
    heap_ident: sql.Composable,
    join_sf: sql.Composable | None,
    sw: sql.Composable,
    heap_fw: sql.Composable | None = None,
    candidate_limit: int | None = None,
    scored_order: sql.Composable | None = None,
    candidate_order_asc: bool = False,
    first_in_with: bool = False,
) -> sql.Composable:
    """``, scored AS (SELECT keys, rank FROM heap [JOIN filtered] WHERE match)``."""

    prefix = sql.SQL("") if first_in_with else sql.SQL(",")

    if join_sf is not None:
        from_sql = sql.SQL(
            """
                FROM {heap} {ia}
                INNER JOIN {filtered} {fa} ON ({join_sf})
                """
        ).format(
            heap=heap_ident,
            ia=sql.Identifier(aliases.index),
            filtered=sql.Identifier(aliases.filtered),
            fa=sql.Identifier(aliases.filtered),
            join_sf=join_sf,
        )

    else:
        from_sql = sql.SQL(" FROM {heap} {ia} ").format(
            heap=heap_ident,
            ia=sql.Identifier(aliases.index),
        )

    where_parts: list[sql.Composable] = [sw]

    if heap_fw is not None:
        where_parts.append(heap_fw)

    where_sql = sql.SQL(" AND ").join(where_parts)

    tail = sql.SQL("")

    if candidate_limit is not None and scored_order is not None:
        order_suf = (
            sql.SQL("ASC NULLS LAST")
            if candidate_order_asc
            else sql.SQL("DESC NULLS LAST")
        )
        tail = sql.SQL(" ORDER BY {ord} {suf} LIMIT {lim}").format(  # type: ignore[assignment]
            ord=scored_order,
            suf=order_suf,
            lim=sql.Literal(int(candidate_limit)),
        )

    return sql.SQL(
        """
            {prefix}
            {scored} AS (
                SELECT {scored_keys}, {scored_rank}
                {from_sql}
                WHERE {where_sql}{tail}
            )"""
    ).format(
        prefix=prefix,
        scored=sql.Identifier(aliases.scored),
        scored_keys=scored_keys,
        scored_rank=scored_rank,
        from_sql=from_sql,
        where_sql=where_sql,
        tail=tail,
    )


# ....................... #


def build_pgroonga_index_first_pipeline(
    *,
    aliases: PipelineAliases,
    scored_keys: sql.Composable,
    scored_rank: sql.Composable,
    heap_ident: sql.Composable,
    sw: sql.Composable,
    join_vs: sql.Composable,
    proj_ident: sql.Composable,
    proj_fw: sql.Composable,
    heap_row_limit: int | None,
    scored_order: sql.Composable | None,
) -> tuple[sql.Composable, sql.Composable]:
    """Index-first PGroonga: top-K on heap, then join projection with filters.

  When ``heap_row_limit`` is ``None``, the scored CTE has no ``LIMIT`` (for exact counts).

    Returns ``(with_clause, from_outer)``.
    """

    where_parts: list[sql.Composable] = [sw]
    tail: sql.Composable = sql.SQL("")

    if heap_row_limit is not None and scored_order is not None:
        tail = sql.SQL(" ORDER BY {ord} DESC NULLS LAST LIMIT {lim}").format(
            ord=scored_order,
            lim=sql.Literal(int(heap_row_limit)),
        )

    scored_cte = sql.SQL(
        """
            {scored} AS (
                SELECT {scored_keys}, {scored_rank}
                FROM {heap} {ia}
                WHERE {sw}{tail}
            )"""
    ).format(
        scored=sql.Identifier(aliases.scored),
        scored_keys=scored_keys,
        scored_rank=scored_rank,
        heap=heap_ident,
        ia=sql.Identifier(aliases.index),
        sw=sql.SQL(" AND ").join(where_parts),
        tail=tail,
    )

    from_outer = sql.SQL(
        """
            FROM {proj} {pa}
            INNER JOIN {scored} {sa} ON ({join_vs})
            WHERE {fw}
            """
    ).format(
        proj=proj_ident,
        pa=sql.Identifier(aliases.projection),
        scored=sql.Identifier(aliases.scored),
        sa=sql.Identifier(aliases.scored),
        join_vs=join_vs,
        fw=proj_fw,
    )

    with_clause = sql.SQL("WITH {}{}").format(scored_cte, sql.SQL(""))

    return with_clause, from_outer


# ....................... #


def build_outer_from(
    *,
    aliases: PipelineAliases,
    proj_ident: sql.Composable,
    join_vs: sql.Composable,
) -> sql.Composable:
    """``FROM projection INNER JOIN scored ON (...)``."""

    return sql.SQL(
        """
            FROM {proj} {pa}
            INNER JOIN {scored} {sa} ON ({join_vs})
            """
    ).format(
        proj=proj_ident,
        pa=sql.Identifier(aliases.projection),
        scored=sql.Identifier(aliases.scored),
        sa=sql.Identifier(aliases.scored),
        join_vs=join_vs,
    )


# ....................... #


def build_rank_first_order(
    *,
    aliases: PipelineAliases,
    extra_order: sql.Composable | None,
) -> sql.Composable:
    """``s.<rank> DESC NULLS LAST`` plus optional projection sort."""

    order_parts: list[sql.Composable] = [
        sql.SQL("{} DESC NULLS LAST").format(
            sql.Identifier(aliases.scored, aliases.rank_column),
        )
    ]

    if extra_order is not None:
        order_parts.append(extra_order)

    return sql.SQL(", ").join(order_parts)


# ....................... #


def build_pipeline_with_clause(
    filtered_cte: sql.Composable,
    scored_cte: sql.Composable,
) -> sql.Composable:
    """``WITH <filtered><scored>`` (scored fragment includes leading comma)."""

    return sql.SQL("WITH {}{}").format(filtered_cte, scored_cte)
