"""Hub search SQL helpers (WITH/combo CTE assembly)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence, cast

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryFilterExpression,
    QuerySortExpression,
    normalize_sorts_for_keyset,
    resolve_effective_sorts,
)
from forze.application.contracts.search import (
    SearchOptions,
    ranked_search_cursor_key_spec,
)
from forze.domain.constants import ID_FIELD

from ._typing_host import HubSearchHost
from .constants import (
    COMBO_ALIAS,
    HUB_CTE,
    HUB_GROONGA_CTID,
    HUB_GROONGA_TABLEOID,
    HUB_RANK,
    HUB_ROW_ALIAS,
    LEG_EID,
    LEG_SCORE,
)
from .runtime import hub_leg_engine_for

# ----------------------- #


class HubSearchSqlMixin[M: BaseModel]:
    """SQL building methods shared by hub offset and cursor search."""

    @property
    def _hub_host(self) -> HubSearchHost[M]:
        return cast(HubSearchHost[M], self)

    # ....................... #

    def _hub_select_list(self, *, include_groonga_sys: bool) -> sql.Composable:
        host = self._hub_host
        base = sql.SQL(", ").join(
            sql.Identifier(HUB_ROW_ALIAS, f) for f in sorted(host.read_fields)
        )

        if not include_groonga_sys:
            return base

        ha = sql.Identifier(HUB_ROW_ALIAS)
        ext = sql.SQL("{}, {}").format(
            sql.SQL("{}.tableoid AS {}").format(
                ha, sql.Identifier(HUB_GROONGA_TABLEOID)
            ),
            sql.SQL("{}.ctid AS {}").format(ha, sql.Identifier(HUB_GROONGA_CTID)),
        )

        return sql.SQL("{}, {}").format(base, ext)

    # ....................... #

    async def _hub_order_by(
        self,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable | None:
        return await self._hub_host.order_by_clause(sorts, table_alias=COMBO_ALIAS)

    # ....................... #

    async def _hub_order_sql_for_search(
        self,
        do_legs: bool,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable:
        if do_legs:
            order_parts: list[sql.Composable] = [
                sql.SQL("{} DESC NULLS LAST").format(
                    sql.Identifier(COMBO_ALIAS, HUB_RANK),
                )
            ]

            ob = await self._hub_order_by(sorts)

            if ob is not None:
                order_parts.append(ob)

            return sql.SQL(", ").join(order_parts)

        ob = await self._hub_order_by(sorts)

        if ob is not None:
            order_parts = [ob]

        elif ID_FIELD in self._hub_host.read_fields:
            order_parts = [
                sql.SQL("{} ASC").format(
                    sql.Identifier(COMBO_ALIAS, ID_FIELD),
                ),
            ]

        else:
            first = sorted(self._hub_host.read_fields)[0]
            order_parts = [
                sql.SQL("{} ASC").format(
                    sql.Identifier(COMBO_ALIAS, first),
                ),
            ]

        return sql.SQL(", ").join(order_parts)

    # ....................... #

    async def _hub_build_with_clause(
        self,
        *,
        query_terms: tuple[str, ...],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        leg_options: SearchOptions | None,
        member_weights_list: Sequence[float],
    ) -> tuple[sql.Composable, list[Any], bool]:
        fw, fp = await self._hub_host.where_clause(filters)

        active = [
            (i, leg, member_weights_list[i])
            for i, leg in enumerate(self._hub_host.members)
            if member_weights_list[i] > 0.0
        ]

        do_legs = bool(query_terms) and bool(active)
        need_groonga_sys = do_legs and any(
            leg.same_heap_as_hub and leg.engine == "pgroonga" for _, leg, _ in active
        )

        hub_cte = sql.SQL(
            """
            {hub_cte} AS (
                SELECT {hub_cols}
                FROM {hub_rel} {ha}
                WHERE {fw}
            )
            """
        ).format(
            hub_cte=sql.Identifier(HUB_CTE),
            hub_cols=self._hub_select_list(include_groonga_sys=need_groonga_sys),
            hub_rel=self._hub_host.source_qname.ident(),
            ha=sql.Identifier(HUB_ROW_ALIAS),
            fw=fw,
        )

        params: list[Any] = [*fp]
        leg_cte_parts: list[sql.Composable] = []
        leg_aliases = [f"lr{i}" for i in range(len(self._hub_host.members))]

        if do_legs:
            for i, leg, _ in active:
                t_alias = HUB_ROW_ALIAS if leg.same_heap_as_hub else f"t{i}"
                lr_alias = leg_aliases[i]

                v_emb = (
                    self._hub_host.vector_embedders.get(i)
                    if leg.engine == "vector"
                    else None
                )
                sw, rank_expr, sp = await hub_leg_engine_for(
                    leg,
                    vector_embedder=v_emb,
                ).build_leg(
                    leg,
                    introspector=self._hub_host.introspector,
                    index_alias=t_alias,
                    queries=query_terms,
                    options=leg_options,
                    score_column=LEG_SCORE,
                )
                params.extend(sp)

                if leg.same_heap_as_hub and leg.engine == "pgroonga" and query_terms:
                    rank_expr = sql.SQL("pgroonga_score({}.{}, {}.{}) AS {}").format(
                        sql.Identifier(t_alias),
                        sql.Identifier(HUB_GROONGA_TABLEOID),
                        sql.Identifier(t_alias),
                        sql.Identifier(HUB_GROONGA_CTID),
                        sql.Identifier(LEG_SCORE),
                    )

                sel_pk = sql.SQL("{} AS {}").format(
                    sql.SQL("{}.{}").format(
                        sql.Identifier(t_alias),
                        sql.Identifier(leg.heap_pk_column),
                    ),
                    sql.Identifier(LEG_EID),
                )

                if leg.same_heap_as_hub:
                    leg_cte = sql.SQL(
                        """
                        ,
                        {lr} AS (
                            SELECT {sel_pk}, {rank_expr}
                            FROM {hf} {t}
                            WHERE {sw}
                        )
                        """
                    ).format(
                        lr=sql.Identifier(lr_alias),
                        sel_pk=sel_pk,
                        rank_expr=rank_expr,
                        hf=sql.Identifier(HUB_CTE),
                        t=sql.Identifier(t_alias),
                        sw=sw,
                    )

                else:
                    cand_sub = leg.candidate_subquery(csub_alias=f"csub{i}")
                    join_on = sql.SQL("{} = {}").format(
                        sql.Identifier(t_alias, leg.heap_pk_column),
                        sql.Identifier(f"csub{i}", "cand_id"),
                    )
                    leg_cte = sql.SQL(
                        """
                        ,
                        {lr} AS (
                            SELECT {sel_pk}, {rank_expr}
                            FROM {heap} {t}
                            INNER JOIN {cand} ON ({join_on})
                            WHERE {sw}
                        )
                        """
                    ).format(
                        lr=sql.Identifier(lr_alias),
                        sel_pk=sel_pk,
                        rank_expr=rank_expr,
                        heap=leg.index_heap_qname.ident(),
                        t=sql.Identifier(t_alias),
                        sw=sw,
                        cand=cand_sub,
                        join_on=join_on,
                    )

                leg_cte_parts.append(leg_cte)

                if len(leg.hub_fk_columns) > 1:
                    leg_cte_parts.append(
                        leg.leg_u_cte(
                            leg_cte_alias=lr_alias,
                            u_cte_name=f"{lr_alias}_u",
                        ),
                    )

        hf_cols = sql.SQL(", ").join(
            [
                sql.SQL("{}.{}").format(sql.Identifier(HUB_CTE), sql.Identifier(f))
                for f in sorted(self._hub_host.read_fields)
            ]
        )

        merge_expr: sql.Composable

        if not do_legs:
            merge_expr = sql.SQL("(0)::double precision")
            combine_sql = sql.SQL("TRUE")

            combo_cte = sql.SQL(
                """
                ,
                {combo} AS (
                    SELECT {hf_cols}, {merge} AS {rank}
                    FROM {hf}
                    WHERE {combine}
                )
                """
            ).format(
                combo=sql.Identifier("combo"),
                hf_cols=hf_cols,
                merge=merge_expr,
                rank=sql.Identifier(HUB_RANK),
                hf=sql.Identifier(HUB_CTE),
                combine=combine_sql,
            )

        else:
            score_terms = [
                sql.SQL("({}) * {}").format(
                    leg.merge_coalesce(i),
                    sql.Literal(float(w)),
                )
                for i, leg, w in active
            ]

            if self._hub_host.score_merge == "max":
                merge_expr = sql.SQL("GREATEST({})").format(
                    sql.SQL(", ").join(score_terms),
                )

            else:
                merge_expr = sql.SQL("({})").format(sql.SQL(" + ").join(score_terms))

            join_parts: list[sql.Composable] = []

            for i, leg, _ in active:
                if len(leg.hub_fk_columns) == 1:
                    join_parts.append(
                        leg.equi_pick_join(
                            leg_cte_alias=leg_aliases[i],
                            pick_alias=f"lp{i}",
                        ),
                    )

                else:
                    join_parts.append(
                        leg.multi_equi_pick_join(
                            leg_u_cte=f"{leg_aliases[i]}_u",
                            base_pick_prefix=f"lp{i}",
                        ),
                    )

            leg_joins = sql.SQL(" ").join(join_parts)
            leg_null_checks = [leg.merge_matched(i) for i, leg, _ in active]

            if self._hub_host.combine == "or":
                combine_sql = sql.SQL(" OR ").join(leg_null_checks)  # type: ignore[assignment]

            else:
                combine_sql = sql.SQL(" AND ").join(leg_null_checks)  # type: ignore[assignment]

            combo_cte = sql.SQL(
                """
                ,
                {combo} AS (
                    SELECT {hf_cols}, {merge} AS {rank}
                    FROM {hf}
                    {leg_joins}
                    WHERE {combine}
                )
                """
            ).format(
                combo=sql.Identifier("combo"),
                hf_cols=hf_cols,
                merge=merge_expr,
                rank=sql.Identifier(HUB_RANK),
                hf=sql.Identifier(HUB_CTE),
                leg_joins=leg_joins,
                combine=combine_sql,
            )

        with_clause = sql.SQL("WITH {}{}{}").format(
            hub_cte,
            sql.SQL("").join(leg_cte_parts),
            combo_cte,
        )

        return with_clause, params, do_legs

    # ....................... #

    def _hub_cursor_key_spec(
        self,
        *,
        do_legs: bool,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> list[tuple[str, str]]:
        if not do_legs:
            effective = resolve_effective_sorts(
                sorts=sorts,
                default_sort=self._hub_host.hub_spec.default_sort,
                read_fields=self._hub_host.read_fields,
                spec_name=self._hub_host.hub_spec.name,
            )
            return list(
                normalize_sorts_for_keyset(
                    effective,
                    read_fields=self._hub_host.read_fields,
                )
            )

        user_sorts = sorts if sorts else self._hub_host.hub_spec.default_sort

        return ranked_search_cursor_key_spec(
            rank_field=HUB_RANK,
            sorts=user_sorts,
            read_fields=self._hub_host.read_fields,
        )

    # ....................... #

    @staticmethod
    def _hub_cursor_order_sql(
        exprs: list[sql.Composable],
        sort_keys: list[str],
        directions: list[str],
        *,
        flip: bool,
    ) -> sql.Composable:
        parts: list[sql.Composable] = []

        for ex, d_raw, sk in zip(exprs, directions, sort_keys, strict=True):
            d = ("desc" if d_raw == "asc" else "asc") if flip else d_raw

            if sk == HUB_RANK:
                if d == "desc":
                    parts.append(sql.SQL("{} DESC NULLS LAST").format(ex))

                else:
                    parts.append(sql.SQL("{} ASC NULLS FIRST").format(ex))

            else:
                suf = "ASC" if d == "asc" else "DESC"
                parts.append(sql.SQL("{} {}").format(ex, sql.SQL(suf)))

        return sql.SQL(", ").join(parts)
