"""Parallel per-leg hub search execution."""

from __future__ import annotations

import asyncio
from typing import Any, Sequence, TypeVar, cast

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCursorPage,
    search_page_from_limit_offset,
)
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    encode_keyset_v1,
    row_passes_keyset_seek,
    row_value_for_sort_key,
    validate_cursor_token,
)
from forze.application.contracts.search import SearchOptions, SearchResultSnapshotOptions
from forze.application.integrations.search import (
    SearchResultSnapshot,
    decrypt_search_rows,
)
from forze.base.primitives import JsonDict

from .._cursor_run import parse_search_cursor
from .._materialize_hits import decode_search_hits, materialize_search_page, search_trust_source
from .._search_count import resolve_ranked_approximate_total
from ._leg_sql import HubLegSqlContext, build_hub_cte, build_hub_leg_sql_parts
from ._typing_host import HubSearchHost
from .constants import HUB_CTE, HUB_RANK, HUB_ROW_ALIAS, LEG_EID, LEG_SCORE
from .merge import hub_row_for_materialize
from .plan import HubSearchPlan
from .runtime import HubLegRuntime
from .semantics import merge_hub_combo_rows, merge_hub_leg_row_lists, sort_hub_rows
from .sql import HubSearchSqlMixin, hub_leg_order_limit

M = TypeVar("M", bound=BaseModel)

# ----------------------- #


class HubParallelSearchMixin(HubSearchSqlMixin[M]):
    """Run hub legs as separate ranked queries and merge in Python."""

    async def _hub_parallel_merged_rows(
        self,
        plan: HubSearchPlan,
        *,
        filters: QueryFilterExpression | None,
    ) -> list[dict[str, Any]]:
        host = cast(HubSearchHost[M], self)

        if not plan.do_legs:
            return []

        fw, fp = await host.where_clause(filters)
        tenant_id = host._tenant_id_for_resolve()  # type: ignore[protected-access]
        hub_qn = await host._qname()  # type: ignore[protected-access]

        active = list(plan.active)
        per_leg_limit = plan.per_leg_limit

        hub_cols = sql.SQL(", ").join(
            sql.Identifier(HUB_ROW_ALIAS, f) for f in sorted(host.read_fields)
        )
        materialized = bool(getattr(host, "parallel_hub_cte_materialized", True))

        leg_ctx = HubLegSqlContext(
            hub_rel_ident=hub_qn.ident(),
            fw=fw,
            tenant_id=tenant_id,
            query_terms=plan.terms,
            leg_options=plan.leg_options,
            per_leg_limit=per_leg_limit,
            introspector=host.introspector,
            vector_embedders=dict(host.vector_embedders),
        )

        hub_cte_body = build_hub_cte(
            hub_cols=hub_cols,
            hub_rel_ident=hub_qn.ident(),
            fw=fw,
            materialized=materialized,
        )

        use_single_fk_join = all(len(leg.hub_fk_columns) == 1 for _, leg, _ in active)

        async def _fetch_leg_ranked(
            leg_index: int,
            leg: HubLegRuntime,
        ) -> dict[Any, float]:
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
            stmt = sql.SQL(
                """
                WITH {hub_cte},
                leg AS (
                    SELECT {sel_pk}, {rank_expr}
                    {leg_from}
                    WHERE {sw}
                    {leg_order}
                )
                SELECT leg.{eid} AS {eid_out}, leg.{sc} AS {sc_out}
                FROM leg
                """
            ).format(
                hub_cte=hub_cte_body,
                sel_pk=parts.sel_pk,
                rank_expr=parts.rank_expr,
                leg_from=parts.leg_from,
                sw=parts.sw,
                leg_order=leg_order,
                eid=sql.Identifier(LEG_EID),
                sc=sql.Identifier(LEG_SCORE),
                eid_out=sql.Identifier(LEG_EID),
                sc_out=sql.Identifier(LEG_SCORE),
            )
            params = [*fp, *parts.leg_params]
            rows = await host.client.fetch_all(stmt, params, row_factory="dict")
            return {row[LEG_EID]: float(row[LEG_SCORE]) for row in rows}

        if use_single_fk_join:

            async def _run_leg_joined(
                leg_index: int,
                leg: HubLegRuntime,
            ) -> list[dict[str, Any]]:
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
                *[_run_leg_joined(i, leg) for i, leg, _ in active],
            )
            weights = [w for _, _, w in active]
            merged = merge_hub_leg_row_lists(
                leg_rows=leg_row_lists,
                weights=weights,
                score_merge=plan.score_merge,
                combine=plan.combine,
                read_fields=plan.read_fields,
                rank_field=plan.rank_field,
            )

        else:
            leg_maps = await asyncio.gather(
                *[_fetch_leg_ranked(i, leg) for i, leg, _ in active],
            )

            # Restrict the hub scan to rows at least one leg actually matched (its FK
            # equals a leg eid). ``merge_hub_combo_rows`` drops rows matching no leg
            # anyway, so this only avoids hydrating the heavy read-field columns for
            # rows that cannot survive — bounding the fetch to the union of the per-leg
            # (``per_leg_limit``-capped) result sets instead of the entire filtered hub
            # relation. Each placeholder binds one leg's eid array.
            restrict_parts: list[sql.Composable] = []
            restrict_params: list[Any] = []

            for (_, leg, _), leg_map in zip(active, leg_maps, strict=True):
                if not leg_map:
                    continue

                eids = list(leg_map.keys())

                for col in leg.hub_fk_columns:
                    restrict_parts.append(
                        sql.SQL("{ha}.{col} = ANY({ph})").format(
                            ha=sql.Identifier(HUB_ROW_ALIAS),
                            col=sql.Identifier(col),
                            ph=sql.Placeholder(),
                        )
                    )
                    restrict_params.append(eids)

            hub_rows: list[dict[str, Any]]

            if not restrict_parts:
                # No leg matched anything — no hub row can survive the merge.
                hub_rows = []

            else:
                hub_stmt = sql.SQL(
                    """
                    WITH {hub_cte}
                    SELECT {hub_cols}
                    FROM {hf} {ha}
                    WHERE {restrict}
                    """
                ).format(
                    hub_cte=hub_cte_body,
                    hub_cols=hub_cols,
                    hf=sql.Identifier(HUB_CTE),
                    ha=sql.Identifier(HUB_ROW_ALIAS),
                    restrict=sql.SQL(" OR ").join(restrict_parts),
                )
                hub_rows = await host.client.fetch_all(
                    hub_stmt, [*fp, *restrict_params], row_factory="dict"
                )

            leg_ranked = [
                (leg, m) for (_, leg, _), m in zip(active, leg_maps, strict=True)
            ]
            weights = [w for _, _, w in active]
            merged = merge_hub_combo_rows(
                hub_rows=hub_rows,
                leg_ranked=leg_ranked,
                weights=weights,
                score_merge=plan.score_merge,
                combine=plan.combine,
                read_fields=plan.read_fields,
                rank_field=plan.rank_field,
            )

        sort_hub_rows(merged, key_spec=plan.order_key_spec)
        return merged

    # ....................... #

    async def _hub_parallel_offset_search(
        self,
        *,
        plan: HubSearchPlan,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
        snapshot: SearchResultSnapshotOptions | None,
        return_count: bool,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
        hub_spec: Any,
        result_snapshot: SearchResultSnapshot | None,
    ) -> Any:
        _ = query, sorts, options, snapshot
        host = cast(HubSearchHost[M], self)

        merged = await self._hub_parallel_merged_rows(plan, filters=filters)

        resolved_combo = plan.resolved_combo
        count_policy = plan.count_policy
        total = 0

        if return_count and count_policy != "none":
            if count_policy == "approximate":
                fw, fp = await host.where_clause(filters)
                hub_qn = await host._qname()  # type: ignore[protected-access]
                total = await resolve_ranked_approximate_total(
                    introspector=host.introspector,
                    schema=hub_qn.schema,
                    relation=hub_qn.name,
                    where_sql=fw,
                    params=fp,
                    combo_limit=resolved_combo,
                )
            else:
                total = await self._hub_sql_combo_count_for_plan(
                    plan,
                    filters=filters,
                )

        if resolved_combo is not None:
            merged = merged[:resolved_combo]

        pagination_dict: dict[str, Any] = dict(pagination or {})
        offset = int(pagination_dict.get("offset") or 0)
        limit_raw = pagination_dict.get("limit")

        if limit_raw is None:
            page_limit = len(merged)

        else:
            page_limit = int(cast(int | str, limit_raw))

        page_rows = [
            hub_row_for_materialize(r)
            for r in merged[offset : offset + page_limit]
        ]
        trust = search_trust_source(host.read_validation)
        # Decrypt sealed hub-row fields once, before materialization (no-op if plaintext).
        page_rows, decode_codec = await decrypt_search_rows(
            hub_spec.resolved_read_codec, page_rows
        )

        page = materialize_search_page(
            page_rows=page_rows,
            pool=None,
            u=0,
            page_limit=page_limit,
            return_type=return_type,
            return_fields=return_fields,
            model_type=host.model_type,
            codec=decode_codec,
            trust_source=trust,
        )

        if return_count:
            return search_page_from_limit_offset(
                page,
                pagination_dict,
                total=total if count_policy != "none" else None,
            )

        return search_page_from_limit_offset(page, pagination_dict, total=None)

    # ....................... #

    async def _hub_parallel_cursor_search(
        self,
        *,
        plan: HubSearchPlan,
        filters: QueryFilterExpression | None,
        cursor: CursorPaginationExpression | None,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
        hub_spec: Any,
    ) -> Any:
        host = cast(HubSearchHost[M], self)

        c = dict(cursor or {})
        lim, use_after, use_before = parse_search_cursor(cursor)

        merged = await self._hub_parallel_merged_rows(plan, filters=filters)

        if plan.resolved_combo is not None:
            merged = merged[: plan.resolved_combo]

        sort_keys = [k for k, _ in plan.order_key_spec]
        directions = [d for _, d in plan.order_key_spec]

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            cursor_vals = validate_cursor_token(
                token,
                sort_keys=sort_keys,
                directions=directions,
            )

            merged = [
                r
                for r in merged
                if row_passes_keyset_seek(
                    r,
                    sort_keys=sort_keys,
                    directions=directions,
                    cursor_values=cursor_vals,
                    after=use_after,
                )
            ]

        has_more = len(merged) > lim
        rows = merged[-lim:] if use_before else merged[:lim]

        if use_before:
            rows = list(reversed(rows))

        def _row_token_vals(row: JsonDict) -> list[Any]:
            return [row_value_for_sort_key(row, k) for k in sort_keys]

        if has_more and rows:
            nxt = encode_keyset_v1(
                sort_keys=sort_keys,
                directions=directions,
                values=_row_token_vals(rows[-1]),
            )

        else:
            nxt = None

        if rows and (use_after or (use_before and has_more)):
            prv = encode_keyset_v1(
                sort_keys=sort_keys,
                directions=directions,
                values=_row_token_vals(rows[0]),
            )

        else:
            prv = None

        trust = search_trust_source(host.read_validation)
        # Decrypt sealed hub-row fields once, so a raw field projection and a decoded
        # return_type both read plaintext (no-op if plaintext).
        mat_rows, decode_codec = await decrypt_search_rows(
            hub_spec.resolved_read_codec,
            [hub_row_for_materialize(r) for r in rows],
        )

        if return_fields is not None:
            rj = [{k: r.get(k, None) for k in return_fields} for r in mat_rows]
            return SearchCursorPage(
                hits=rj, next_cursor=nxt, prev_cursor=prv, has_more=has_more
            )

        hits = decode_search_hits(
            rows=mat_rows,
            model_type=host.model_type,
            codec=decode_codec,
            return_type=return_type,
            trust_source=trust,
        )

        return SearchCursorPage(hits=hits, next_cursor=nxt, prev_cursor=prv, has_more=has_more)
