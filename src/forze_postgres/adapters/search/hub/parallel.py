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
from .._search_count import effective_search_count, resolve_ranked_approximate_total
from ._leg_sql import HubLegSqlContext, build_hub_cte, build_hub_leg_sql_parts
from ._typing_host import HubSearchHost
from .constants import HUB_CTE, HUB_RANK, HUB_ROW_ALIAS, LEG_EID, LEG_SCORE
from .merge import merge_hub_leg_rows
from .runtime import HubLegRuntime
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

        materialized = bool(
            getattr(host, "parallel_hub_cte_materialized", True),
        )

        leg_ctx = HubLegSqlContext(
            hub_rel_ident=hub_qn.ident(),
            fw=fw,
            tenant_id=tenant_id,
            query_terms=tuple(terms),
            leg_options=leg_options,
            per_leg_limit=per_leg_limit,
            introspector=host.introspector,
            vector_embedders=vector_embedders,
        )

        hub_cte_body = build_hub_cte(
            hub_cols=hub_cols,
            hub_rel_ident=hub_qn.ident(),
            fw=fw,
            materialized=materialized,
        )

        async def _run_leg(
            leg_index: int,
            leg: HubLegRuntime,
        ) -> list[dict[str, Any]]:
            if len(leg.hub_fk_columns) != 1:
                raise exc.internal(
                    "parallel hub execution requires single-column hub_fk on each leg.",
                )

            parts = await build_hub_leg_sql_parts(
                leg_ctx,
                leg_index=leg_index,
                leg=leg,
                lr_alias="leg",
                off_heap_csub_alias="csub",
            )
            leg_order = hub_leg_order_limit(
                engine=leg.engine,
                per_leg_limit=per_leg_limit,
            )

            fk_join = leg.hub_fk_columns[0]
            stmt = sql.SQL(
                """
                WITH {hub_cte},
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
                hub_cte=hub_cte_body,
                sel_pk=parts.sel_pk,
                rank_expr=parts.rank_expr,
                leg_from=parts.leg_from,
                sw=parts.sw,
                leg_order=leg_order,
                hub_cols=hub_cols,
                hf=sql.Identifier(HUB_CTE),
                ha=sql.Identifier(HUB_ROW_ALIAS),
                sc=sql.Identifier(LEG_SCORE),
                hr=sql.Identifier(HUB_RANK),
                fk=sql.Identifier(fk_join),
                eid=sql.Identifier(LEG_EID),
            )

            params = [*fp, *parts.leg_params]
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

        count_policy = effective_search_count(options)
        full_merged_len = len(merged)
        total = 0

        if return_count and count_policy != "none":
            if count_policy == "approximate":
                total = await resolve_ranked_approximate_total(
                    introspector=host.introspector,
                    schema=hub_qn.schema,
                    relation=hub_qn.name,
                    where_sql=fw,
                    params=fp,
                    combo_limit=resolved_combo,
                )
            else:
                total = full_merged_len

        if resolved_combo is not None:
            merged = merged[:resolved_combo]

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
