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
    COMBO_TOP_RELATION,
    HUB_CTE,
    HUB_GROONGA_CTID,
    HUB_GROONGA_TABLEOID,
    HUB_RANK,
    HUB_ROW_ALIAS,
)
from ._leg_sql import HubLegSqlContext, build_hub_cte, build_hub_leg_sql_parts

# Re-export for parallel and tests.
from ._leg_sql import hub_leg_order_limit as hub_leg_order_limit

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
        per_leg_limit: int,
        combo_limit: int | None = None,
    ) -> tuple[sql.Composable, list[Any], bool, str, str]:
        fw, fp = await self._hub_host.where_clause(filters)
        tenant_id = (
            self._hub_host._tenant_id_for_resolve()  # pyright: ignore[reportPrivateUsage]
        )
        hub_qn = await self._hub_host._qname()  # pyright: ignore[reportPrivateUsage]

        active = [
            (i, leg, member_weights_list[i])
            for i, leg in enumerate(self._hub_host.members)
            if member_weights_list[i] > 0.0
        ]

        do_legs = bool(query_terms) and bool(active)
        need_groonga_sys = False

        hub_cte = build_hub_cte(
            hub_cols=self._hub_select_list(include_groonga_sys=need_groonga_sys),
            hub_rel_ident=hub_qn.ident(),
            fw=fw,
        )

        params: list[Any] = [*fp]
        leg_cte_parts: list[sql.Composable] = []
        leg_aliases = [f"lr{i}" for i in range(len(self._hub_host.members))]

        leg_ctx = HubLegSqlContext(
            hub_rel_ident=hub_qn.ident(),
            fw=fw,
            tenant_id=tenant_id,
            query_terms=query_terms,
            leg_options=leg_options,
            per_leg_limit=per_leg_limit,
            introspector=self._hub_host.introspector,
            vector_embedders=dict(self._hub_host.vector_embedders),
        )

        if do_legs:
            for i, leg, _ in active:
                lr_alias = leg_aliases[i]
                parts = await build_hub_leg_sql_parts(
                    leg_ctx,
                    leg_index=i,
                    leg=leg,
                    lr_alias=lr_alias,
                )
                params.extend(parts.leg_params)
                leg_cte_parts.extend(parts.leg_cte_fragments)

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

        count_relation = "combo"
        data_relation = "combo"
        combo_tail: sql.Composable = sql.SQL("")

        if combo_limit is not None and do_legs:
            combo_top_cte = sql.SQL(
                """
                ,
                {combo_top} AS (
                    SELECT *
                    FROM {combo}
                    ORDER BY {rank} DESC NULLS LAST
                    LIMIT {lim}
                )
                """
            ).format(
                combo_top=sql.Identifier(COMBO_TOP_RELATION),
                combo=sql.Identifier("combo"),
                rank=sql.Identifier(HUB_RANK),
                lim=sql.Literal(int(combo_limit)),
            )
            combo_tail = combo_top_cte
            data_relation = COMBO_TOP_RELATION

        with_clause = sql.SQL("WITH {}{}{}{}").format(
            hub_cte,
            sql.SQL("").join(leg_cte_parts),
            combo_cte,
            combo_tail,
        )

        return with_clause, params, do_legs, count_relation, data_relation

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
