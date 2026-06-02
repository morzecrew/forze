"""Parallel per-leg hub search execution."""

import asyncio
from typing import Any, Literal, Sequence, TypeVar, cast

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.base import page_from_limit_offset
from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    SearchResultSnapshotOptions,
    normalize_search_queries,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import exc

from .._materialize_hits import materialize_search_page
from .._pgroonga_plan import effective_combo_limit
from .._search_count import effective_search_count
from ._typing_host import HubSearchHost
from .constants import HUB_CTE, HUB_RANK, HUB_ROW_ALIAS, LEG_EID, LEG_SCORE
from .merge import merge_hub_leg_rows
from .runtime import HubLegRuntime, hub_leg_engine_for
from .sql import HubSearchSqlMixin, hub_leg_order_limit

M = TypeVar("M", bound=BaseModel)

# ----------------------- #


class HubParallelSearchMixin(HubSearchSqlMixin[M]):
    """Run hub legs as separate ranked queries and merge in Python."""

    async def _hub_parallel_offset_search(
        self,
        *,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
        snapshot: SearchResultSnapshotOptions | None,
        return_count: bool,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
        hub_spec: HubSearchSpec[M],
        members: Sequence[HubLegRuntime],
        vector_embedders: dict[int, Any],
        member_weights_list: Sequence[float],
        score_merge: Literal["max", "sum"],
        combine: Literal["or", "and"],
        per_leg_limit: int,
        combo_limit_config: int | None,
        result_snapshot: SearchResultSnapshot | None,
    ) -> Any:
        _ = sorts
        host = cast(HubSearchHost[M], self)
        terms = normalize_search_queries(query)
        leg_options = options

        fw, fp = await host.where_clause(filters)
        tenant_id = host._tenant_id_for_resolve()  # type: ignore[protected-access]
        hub_qn = await host._qname()  # type: ignore[protected-access]

        active = [
            (i, leg, float(member_weights_list[i]))
            for i, leg in enumerate(members)
            if member_weights_list[i] > 0.0
        ]

        hub_cols = sql.SQL(", ").join(
            sql.Identifier(HUB_ROW_ALIAS, f) for f in sorted(host.read_fields)
        )

        async def _run_leg(
            leg_index: int,
            leg: HubLegRuntime,
        ) -> list[dict[str, Any]]:
            if len(leg.hub_fk_columns) != 1:
                raise exc.internal(
                    "parallel hub execution requires single-column hub_fk on each leg.",
                )

            heap_t_alias = "t"
            t_alias = (
                heap_t_alias
                if leg.same_heap_as_hub and leg.engine == "pgroonga"
                else (HUB_ROW_ALIAS if leg.same_heap_as_hub else f"t{leg_index}")
            )
            leg_order = hub_leg_order_limit(
                engine=leg.engine,
                per_leg_limit=per_leg_limit,
            )

            v_emb = vector_embedders.get(leg_index) if leg.engine == "vector" else None
            sw, rank_expr, sp = await hub_leg_engine_for(
                leg,
                vector_embedder=v_emb,
            ).build_leg(
                leg,
                tenant_id=tenant_id,
                introspector=host.introspector,
                index_alias=t_alias,
                queries=terms,
                options=leg_options,
                score_column=LEG_SCORE,
            )

            if leg.same_heap_as_hub and leg.engine == "pgroonga":
                rank_expr = sql.SQL(
                    "pgroonga_score({}.tableoid, {}.ctid) AS {}"
                ).format(
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
                leg_from = sql.SQL(
                    """
                    FROM {hub_rel} {t}
                    INNER JOIN {hf} {ha} ON ({pk_join})
                    """
                ).format(
                    hub_rel=hub_qn.ident(),
                    t=sql.Identifier(heap_t_alias),
                    hf=sql.Identifier(HUB_CTE),
                    ha=sql.Identifier(HUB_ROW_ALIAS),
                    pk_join=pk_join,
                )
            elif leg.same_heap_as_hub:
                leg_from = sql.SQL(" FROM {hf} {t} ").format(
                    hf=sql.Identifier(HUB_CTE),
                    t=sql.Identifier(t_alias),
                )
            else:
                heap_qn = await leg.resolve_index_heap_qname(tenant_id)
                cand_sub = leg.candidate_subquery(csub_alias="csub")
                join_on = sql.SQL("{} = {}").format(
                    sql.Identifier(t_alias, leg.heap_pk_column),
                    sql.Identifier("csub", "cand_id"),
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

            fk_join = leg.hub_fk_columns[0]
            stmt = sql.SQL(
                """
                WITH {hf} AS (
                    SELECT {hub_cols}
                    FROM {hub_rel} {ha}
                    WHERE {fw}
                ),
                leg AS (
                    SELECT {sel_pk}, {rank_expr}
                    {leg_from}
                    WHERE {sw}
                    {leg_order}
                )
                SELECT {hub_cols}, leg.{sc} AS {hr}
                FROM {hf} {ha}
                INNER JOIN leg ON {ha}.{fk} = leg.{eid}
                """
            ).format(
                hf=sql.Identifier(HUB_CTE),
                hub_cols=hub_cols,
                hub_rel=hub_qn.ident(),
                ha=sql.Identifier(HUB_ROW_ALIAS),
                fw=fw,
                sel_pk=sel_pk,
                rank_expr=rank_expr,
                leg_from=leg_from,
                sw=sw,
                leg_order=leg_order,
                sc=sql.Identifier(LEG_SCORE),
                hr=sql.Identifier(HUB_RANK),
                fk=sql.Identifier(fk_join),
                eid=sql.Identifier(LEG_EID),
            )

            params = [*fp, *sp]
            return await host.client.fetch_all(stmt, params, row_factory="dict")

        leg_row_lists = await asyncio.gather(
            *[_run_leg(i, leg) for i, leg, _ in active],
        )

        active_weights = [w for _, _, w in active]
        merged = merge_hub_leg_rows(
            leg_rows=leg_row_lists,
            weights=active_weights,
            score_merge=score_merge,
            combine=combine,
            read_fields=host.read_fields,
        )

        rs_spec = hub_spec.snapshot
        resolved_combo = effective_combo_limit(
            config_limit=combo_limit_config,
            per_leg_limit=per_leg_limit,
            options=options,
            pagination=dict(pagination or {}),
            snapshot=snapshot,
            result_snapshot=result_snapshot,
            rs_spec=rs_spec,
        )

        if resolved_combo is not None:
            merged = merged[:resolved_combo]

        count_policy = effective_search_count(options)
        total = len(merged) if return_count and count_policy != "none" else 0

        if return_count and count_policy == "approximate":
            total = await host.introspector.estimate_filtered_rows(
                schema=hub_qn.schema,
                relation=hub_qn.name,
                where_sql=fw,
                params=fp,
            )

        pagination_dict: dict[str, Any] = dict(pagination or {})
        offset = int(pagination_dict.get("offset") or 0)
        limit_raw = pagination_dict.get("limit")

        if limit_raw is None:
            page_limit = len(merged)
        else:
            page_limit = int(cast(int | str, limit_raw))

        page_rows = merged[offset : offset + page_limit]

        page = materialize_search_page(
            page_rows=page_rows,
            pool=None,
            u=0,
            page_limit=page_limit,
            return_type=return_type,
            return_fields=return_fields,
            model_type=host.model_type,
            codec=hub_spec.resolved_read_codec,
        )

        if return_count:
            return page_from_limit_offset(
                page,
                pagination_dict,
                total=total if count_policy != "none" else None,
            )

        return page_from_limit_offset(page, pagination_dict, total=None)
