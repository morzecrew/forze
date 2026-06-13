"""Hub search SQL helpers (WITH/combo CTE assembly)."""

from __future__ import annotations

from typing import Any, Mapping, Sequence, cast

from forze_postgres._compat import require_psycopg

require_psycopg()

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.sql.query.nested import sort_key_expr

from ._leg_sql import HubLegSqlContext, build_hub_cte, build_hub_leg_sql_parts

# Re-export for parallel and tests.
from ._leg_sql import hub_leg_order_limit as hub_leg_order_limit
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
from .plan import HubSearchPlan
from .semantics import hub_order_key_spec, sql_combine_where, sql_merge_expr

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
        *,
        table_alias: str = COMBO_ALIAS,
    ) -> sql.Composable | None:
        return await self._hub_host.order_by_clause(sorts, table_alias=table_alias)

    # ....................... #

    async def render_hub_order_sql(
        self,
        plan: HubSearchPlan,
        *,
        table_alias: str = COMBO_ALIAS,
    ) -> sql.Composable:
        if plan.do_legs:
            order_parts: list[sql.Composable] = [
                sql.SQL("{} DESC NULLS LAST").format(
                    sql.Identifier(table_alias, HUB_RANK),
                )
            ]

            ob = await self._hub_order_by(plan.effective_sorts, table_alias=table_alias)

            if ob is not None:
                order_parts.append(ob)

            return sql.SQL(", ").join(order_parts)

        ob = await self._hub_order_by(plan.effective_sorts, table_alias=table_alias)

        if ob is not None:
            order_parts = [ob]

        elif ID_FIELD in self._hub_host.read_fields:
            order_parts = [
                sql.SQL("{} ASC").format(
                    sql.Identifier(table_alias, ID_FIELD),
                ),
            ]

        else:
            first = sorted(self._hub_host.read_fields)[0]
            order_parts = [
                sql.SQL("{} ASC").format(
                    sql.Identifier(table_alias, first),
                ),
            ]

        return sql.SQL(", ").join(order_parts)

    # ....................... #

    async def _hub_order_sql_for_search(
        self,
        do_legs: bool,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable:
        """Backward-compatible wrapper; prefer :meth:`render_hub_order_sql` with a plan."""

        plan = HubSearchPlan(
            terms=(),
            do_legs=do_legs,
            active=(),
            leg_options={},  # type: ignore[arg-type]
            member_weights_list=(),
            combine=self._hub_host.combine,  # type: ignore[arg-type]
            score_merge=self._hub_host.score_merge,  # type: ignore[arg-type]
            read_fields=self._hub_host.read_fields,
            rank_field=HUB_RANK,
            per_leg_limit=self._hub_host.per_leg_limit,
            resolved_combo=None,
            effective_sorts=sorts if sorts else self._hub_host.hub_spec.default_sort,
            order_key_spec=(),
            use_parallel=False,
            count_policy="none",
            execution="sql",
        )
        return await self.render_hub_order_sql(plan)

    # ....................... #

    async def _hub_combo_top_order_by(
        self,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable | None:
        """User sort keys on bare ``combo`` column names (matches ``SELECT * FROM combo``)."""

        if not sorts:
            return None

        host = self._hub_host
        types = await host.column_types()
        parts: list[sql.Composable] = []

        for field, value in sorts.items():
            order = value.get("dir") if isinstance(value, Mapping) else value
            key = sort_key_expr(
                field=field,
                column_types=types,
                model_type=host.model_type,
                nested_field_hints=host.nested_field_hints,
                table_alias=None,
            )
            dir_sql = "ASC" if str(order).lower() == "asc" else "DESC"
            parts.append(sql.SQL("{} {}").format(key, sql.SQL(dir_sql)))

        return sql.SQL(", ").join(parts)

    # ....................... #

    async def _hub_combo_top_order_sql(self, plan: HubSearchPlan) -> sql.Composable:
        """ORDER BY for ``combo_top`` (rank + user keys, aligned with outer hub read)."""

        if plan.do_legs:
            order_parts: list[sql.Composable] = [
                sql.SQL("{} DESC NULLS LAST").format(sql.Identifier(HUB_RANK)),
            ]
            ob = await self._hub_combo_top_order_by(plan.effective_sorts)

            if ob is not None:
                order_parts.append(ob)

            return sql.SQL(", ").join(order_parts)

        return await self.render_hub_order_sql(plan, table_alias=COMBO_ALIAS)

    # ....................... #

    async def _hub_build_with_clause_from_plan(
        self,
        plan: HubSearchPlan,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        combo_limit: int | None = None,
        uncapped_legs: bool = False,
    ) -> tuple[sql.Composable, list[Any], bool, str, str]:
        fw, fp = await self._hub_host.where_clause(filters)
        tenant_id = (
            self._hub_host._tenant_id_for_resolve()  # pyright: ignore[reportPrivateUsage]
        )
        hub_qn = await self._hub_host._qname()  # pyright: ignore[reportPrivateUsage]

        do_legs = plan.do_legs
        active = list(plan.active)
        need_groonga_sys = False

        hub_cte = build_hub_cte(
            hub_cols=self._hub_select_list(include_groonga_sys=need_groonga_sys),
            hub_rel_ident=hub_qn.ident(),
            fw=fw,
        )

        params: list[Any] = [*fp]
        leg_cte_parts: list[sql.Composable] = []
        leg_aliases = [f"lr{i}" for i in range(len(self._hub_host.members))]

        leg_per_leg_limit = None if uncapped_legs else plan.per_leg_limit

        leg_ctx = HubLegSqlContext(
            hub_rel_ident=hub_qn.ident(),
            fw=fw,
            tenant_id=tenant_id,
            query_terms=plan.terms,
            leg_options=plan.leg_options,
            per_leg_limit=leg_per_leg_limit,
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
        combine_sql: sql.Composable

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
            merge_expr = sql_merge_expr(active, plan.score_merge)
            combine_sql = sql_combine_where(active, plan.combine)

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

        effective_combo = (
            combo_limit if combo_limit is not None else plan.resolved_combo
        )

        if effective_combo is not None and do_legs:
            combo_top_order = await self._hub_combo_top_order_sql(plan)
            combo_top_cte = sql.SQL(
                """
                ,
                {combo_top} AS (
                    SELECT *
                    FROM {combo}
                    ORDER BY {order}
                    LIMIT {lim}
                )
                """
            ).format(
                combo_top=sql.Identifier(COMBO_TOP_RELATION),
                combo=sql.Identifier("combo"),
                order=combo_top_order,
                lim=sql.Literal(int(effective_combo)),
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

    async def _hub_build_with_clause(
        self,
        *,
        query_terms: tuple[str, ...],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        leg_options: Any,
        member_weights_list: Sequence[float],
        per_leg_limit: int | None,
        combo_limit: int | None = None,
        sorts: QuerySortExpression | None = None,  # type: ignore[valid-type]
    ) -> tuple[sql.Composable, list[Any], bool, str, str]:
        active = tuple(
            (i, leg, float(member_weights_list[i]))
            for i, leg in enumerate(self._hub_host.members)
            if member_weights_list[i] > 0.0
        )
        do_legs = bool(query_terms) and bool(active)
        effective_sorts = sorts if sorts else self._hub_host.hub_spec.default_sort
        key_spec = hub_order_key_spec(
            do_legs=do_legs,
            sorts=sorts,
            default_sort=self._hub_host.hub_spec.default_sort,
            read_fields=self._hub_host.read_fields,
            spec_name=self._hub_host.hub_spec.name,
            rank_field=HUB_RANK,
        )
        plan = HubSearchPlan(
            terms=query_terms,
            do_legs=do_legs,
            active=active,
            leg_options=leg_options,
            member_weights_list=tuple(float(w) for w in member_weights_list),
            combine=self._hub_host.combine,  # type: ignore[arg-type]
            score_merge=self._hub_host.score_merge,  # type: ignore[arg-type]
            read_fields=self._hub_host.read_fields,
            rank_field=HUB_RANK,
            per_leg_limit=(
                self._hub_host.per_leg_limit if per_leg_limit is None else per_leg_limit
            ),
            resolved_combo=combo_limit,
            effective_sorts=effective_sorts,
            order_key_spec=tuple(key_spec),
            use_parallel=False,
            count_policy="none",
            execution="sql",
        )

        return await self._hub_build_with_clause_from_plan(
            plan,
            filters=filters,
            combo_limit=combo_limit,
            uncapped_legs=per_leg_limit is None,
        )

    # ....................... #

    async def _hub_sql_combo_count_for_plan(
        self,
        plan: HubSearchPlan,
        *,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        combo_alias: str = COMBO_ALIAS,
    ) -> int:
        """Exact ``COUNT(*)`` over the SQL ``combo`` CTE (no ``combo_top`` cap)."""

        count_plan = HubSearchPlan(
            terms=plan.terms,
            do_legs=plan.do_legs,
            active=plan.active,
            leg_options=plan.leg_options,
            member_weights_list=plan.member_weights_list,
            combine=plan.combine,
            score_merge=plan.score_merge,
            read_fields=plan.read_fields,
            rank_field=plan.rank_field,
            per_leg_limit=plan.per_leg_limit,
            resolved_combo=None,
            effective_sorts=plan.effective_sorts,
            order_key_spec=plan.order_key_spec,
            use_parallel=plan.use_parallel,
            count_policy=plan.count_policy,
            execution=plan.execution,
        )
        with_clause, params, _do_legs, count_relation, _data_rel = (
            await self._hub_build_with_clause_from_plan(
                count_plan,
                filters=filters,
                combo_limit=None,
                uncapped_legs=True,
            )
        )
        count_stmt = sql.SQL(
            """
            {with_clause}
            SELECT COUNT(*) FROM {combo} {ca}
            """
        ).format(
            with_clause=with_clause,
            combo=sql.Identifier(count_relation),
            ca=sql.Identifier(combo_alias),
        )
        return int(
            await self._hub_host.client.fetch_value(count_stmt, params, default=0),
        )

    # ....................... #

    async def _hub_sql_combo_count(
        self,
        *,
        query_terms: tuple[str, ...],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        leg_options: Any,
        member_weights_list: Sequence[float],
        per_leg_limit: int,
        sorts: QuerySortExpression | None = None,  # type: ignore[valid-type]
        combo_alias: str = COMBO_ALIAS,
    ) -> int:
        active = tuple(
            (i, leg, float(member_weights_list[i]))
            for i, leg in enumerate(self._hub_host.members)
            if member_weights_list[i] > 0.0
        )
        do_legs = bool(query_terms) and bool(active)
        key_spec = hub_order_key_spec(
            do_legs=do_legs,
            sorts=sorts,
            default_sort=self._hub_host.hub_spec.default_sort,
            read_fields=self._hub_host.read_fields,
            spec_name=self._hub_host.hub_spec.name,
            rank_field=HUB_RANK,
        )
        plan = HubSearchPlan(
            terms=query_terms,
            do_legs=do_legs,
            active=active,
            leg_options=leg_options,
            member_weights_list=tuple(float(w) for w in member_weights_list),
            combine=self._hub_host.combine,  # type: ignore[arg-type]
            score_merge=self._hub_host.score_merge,  # type: ignore[arg-type]
            read_fields=self._hub_host.read_fields,
            rank_field=HUB_RANK,
            per_leg_limit=per_leg_limit,
            resolved_combo=None,
            effective_sorts=sorts if sorts else self._hub_host.hub_spec.default_sort,
            order_key_spec=tuple(key_spec),
            use_parallel=False,
            count_policy="none",
            execution="sql",
        )
        return await self._hub_sql_combo_count_for_plan(
            plan,
            filters=filters,
            combo_alias=combo_alias,
        )

    # ....................... #

    def _hub_cursor_key_spec(
        self,
        *,
        do_legs: bool,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> list[tuple[str, str]]:
        return hub_order_key_spec(
            do_legs=do_legs,
            sorts=sorts,
            default_sort=self._hub_host.hub_spec.default_sort,
            read_fields=self._hub_host.read_fields,
            spec_name=self._hub_host.hub_spec.name,
            rank_field=HUB_RANK,
        )
