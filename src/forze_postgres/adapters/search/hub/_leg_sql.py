"""Shared hub CTE and per-leg SQL fragments (sql + parallel execution)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import attrs
from psycopg import sql

from forze.application.contracts.search import SearchOptions

from .constants import HUB_CTE, HUB_ROW_ALIAS, LEG_EID, LEG_SCORE
from .runtime import HubLegRuntime, hub_leg_engine_for

# ----------------------- #


def hub_leg_order_limit(*, engine: str, per_leg_limit: int) -> sql.Composable:
    """``ORDER BY … LIMIT`` suffix for capped hub leg CTEs."""

    score = sql.Identifier(LEG_SCORE)

    if engine == "vector":
        return sql.SQL(" ORDER BY {} ASC NULLS LAST LIMIT {}").format(
            score,
            sql.Literal(int(per_leg_limit)),
        )

    return sql.SQL(" ORDER BY {} DESC NULLS LAST LIMIT {}").format(
        score,
        sql.Literal(int(per_leg_limit)),
    )


# ----------------------- #


@attrs.define(frozen=True, slots=True, kw_only=True)
class HubLegSqlContext:
    """Inputs shared when building hub filter CTE and leg fragments."""

    hub_rel_ident: sql.Composable
    fw: sql.Composable
    tenant_id: UUID | None
    query_terms: tuple[str, ...]
    leg_options: SearchOptions | None
    per_leg_limit: int | None
    introspector: Any
    vector_embedders: dict[int, Any]


# ....................... #


@attrs.define(frozen=True, slots=True, kw_only=True)
class HubLegSqlParts:
    """Per-leg SQL pieces for CTE (sql) or subquery (parallel) paths."""

    sel_pk: sql.Composable
    rank_expr: sql.Composable
    sw: sql.Composable
    leg_params: list[Any]
    leg_cte_fragments: list[sql.Composable]
    leg_from: sql.Composable


# ....................... #


def build_hub_cte(
    *,
    hub_cols: sql.Composable,
    hub_rel_ident: sql.Composable,
    fw: sql.Composable,
    hub_cte_name: str = HUB_CTE,
    hub_row_alias: str = HUB_ROW_ALIAS,
    materialized: bool = False,
) -> sql.Composable:
    """``{hub_cte} AS [MATERIALIZED] (SELECT … FROM hub WHERE fw)`` fragment."""

    mat = sql.SQL(" MATERIALIZED") if materialized else sql.SQL("")

    return sql.SQL(
        """
            {hub_cte} AS{mat} (
                SELECT {hub_cols}
                FROM {hub_rel} {ha}
                WHERE {fw}
            )
            """
    ).format(
        hub_cte=sql.Identifier(hub_cte_name),
        mat=mat,
        hub_cols=hub_cols,
        hub_rel=hub_rel_ident,
        ha=sql.Identifier(hub_row_alias),
        fw=fw,
    )


# ....................... #


async def build_hub_leg_sql_parts(
    ctx: HubLegSqlContext,
    *,
    leg_index: int,
    leg: HubLegRuntime,
    lr_alias: str,
    off_heap_csub_alias: str | None = None,
) -> HubLegSqlParts:
    """Build leg CTE suffix(es) and parallel ``leg_from`` for one active hub leg."""

    heap_t_alias = "t"

    t_alias = (
        heap_t_alias
        if leg.same_heap_as_hub and leg.engine == "pgroonga"
        else (HUB_ROW_ALIAS if leg.same_heap_as_hub else f"t{leg_index}")
    )

    leg_order = (
        hub_leg_order_limit(
            engine=leg.engine,
            per_leg_limit=ctx.per_leg_limit,
        )
        if ctx.per_leg_limit is not None
        else sql.SQL("")
    )

    v_emb = ctx.vector_embedders.get(leg_index) if leg.engine == "vector" else None

    sw, rank_expr, sp = await hub_leg_engine_for(
        leg,
        vector_embedder=v_emb,
    ).build_leg(
        leg,
        tenant_id=ctx.tenant_id,
        introspector=ctx.introspector,
        index_alias=t_alias,
        queries=ctx.query_terms,
        options=ctx.leg_options,
        score_column=LEG_SCORE,
    )

    if leg.same_heap_as_hub and leg.engine == "pgroonga" and ctx.query_terms:
        rank_expr = sql.SQL("pgroonga_score({}.tableoid, {}.ctid) AS {}").format(
            sql.Identifier(heap_t_alias),
            sql.Identifier(heap_t_alias),
            sql.Identifier(LEG_SCORE),
        )

    sel_pk = sql.SQL("{} AS {}").format(
        sql.SQL("{}.{}").format(
            sql.Identifier(t_alias),
            sql.Identifier(leg.heap_pk_column),
        ),
        sql.Identifier(LEG_EID),
    )

    leg_cte_fragments: list[sql.Composable] = []

    if leg.same_heap_as_hub and leg.engine == "pgroonga":
        pk_join = sql.SQL("{} = {}").format(
            sql.SQL("{}.{}").format(
                sql.Identifier(HUB_ROW_ALIAS),
                sql.Identifier(leg.heap_pk_column),
            ),
            sql.SQL("{}.{}").format(
                sql.Identifier(heap_t_alias),
                sql.Identifier(leg.heap_pk_column),
            ),
        )
        leg_cte = sql.SQL(
            """
                        ,
                        {lr} AS (
                            SELECT {sel_pk}, {rank_expr}
                            FROM {hub_rel} {t}
                            INNER JOIN {hf} {ha} ON ({pk_join})
                            WHERE {sw}{leg_order}
                        )
                        """
        ).format(
            lr=sql.Identifier(lr_alias),
            sel_pk=sel_pk,
            rank_expr=rank_expr,
            hub_rel=ctx.hub_rel_ident,
            t=sql.Identifier(heap_t_alias),
            hf=sql.Identifier(HUB_CTE),
            ha=sql.Identifier(HUB_ROW_ALIAS),
            pk_join=pk_join,
            sw=sw,
            leg_order=leg_order,
        )
        leg_from = sql.SQL(
            """
                    FROM {hub_rel} {t}
                    INNER JOIN {hf} {ha} ON ({pk_join})
                    """
        ).format(
            hub_rel=ctx.hub_rel_ident,
            t=sql.Identifier(heap_t_alias),
            hf=sql.Identifier(HUB_CTE),
            ha=sql.Identifier(HUB_ROW_ALIAS),
            pk_join=pk_join,
        )

    elif leg.same_heap_as_hub:
        leg_cte = sql.SQL(
            """
                        ,
                        {lr} AS (
                            SELECT {sel_pk}, {rank_expr}
                            FROM {hf} {t}
                            WHERE {sw}{leg_order}
                        )
                        """
        ).format(
            lr=sql.Identifier(lr_alias),
            sel_pk=sel_pk,
            rank_expr=rank_expr,
            hf=sql.Identifier(HUB_CTE),
            t=sql.Identifier(t_alias),
            sw=sw,
            leg_order=leg_order,
        )
        leg_from = sql.SQL(" FROM {hf} {t} ").format(
            hf=sql.Identifier(HUB_CTE),
            t=sql.Identifier(t_alias),
        )

    else:
        csub_name = off_heap_csub_alias or f"csub{leg_index}"
        heap_qn = await leg.resolve_index_heap_qname(ctx.tenant_id)
        cand_sub = leg.candidate_subquery(csub_alias=csub_name)
        join_on = sql.SQL("{} = {}").format(
            sql.Identifier(t_alias, leg.heap_pk_column),
            sql.Identifier(csub_name, "cand_id"),
        )
        leg_cte = sql.SQL(
            """
                        ,
                        {lr} AS (
                            SELECT {sel_pk}, {rank_expr}
                            FROM {heap} {t}
                            INNER JOIN {cand} ON ({join_on})
                            WHERE {sw}{leg_order}
                        )
                        """
        ).format(
            lr=sql.Identifier(lr_alias),
            sel_pk=sel_pk,
            rank_expr=rank_expr,
            heap=heap_qn.ident(),
            t=sql.Identifier(t_alias),
            sw=sw,
            cand=cand_sub,
            join_on=join_on,
            leg_order=leg_order,
        )
        leg_from = sql.SQL(
            """
                    FROM {heap} {t}
                    INNER JOIN {cand} ON ({join_on})
                    """
        ).format(
            heap=heap_qn.ident(),
            t=sql.Identifier(t_alias),
            cand=cand_sub,
            join_on=join_on,
        )

    leg_cte_fragments.append(leg_cte)

    if len(leg.hub_fk_columns) > 1:
        leg_cte_fragments.append(
            leg.leg_u_cte(
                leg_cte_alias=lr_alias,
                u_cte_name=f"{lr_alias}_u",
            ),
        )

    return HubLegSqlParts(
        sel_pk=sel_pk,
        rank_expr=rank_expr,
        sw=sw,
        leg_params=list(sp),
        leg_cte_fragments=leg_cte_fragments,
        leg_from=leg_from,
    )
