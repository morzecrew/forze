"""Parallel per-leg hub search execution."""

from __future__ import annotations

import asyncio
from typing import Any, Literal, Sequence, TypeVar, cast

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.base import CursorPage, page_from_limit_offset
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    decode_keyset_v1,
    encode_keyset_v1,
    row_value_for_sort_key,
)
from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    SearchResultSnapshotOptions,
    normalize_search_queries,
    prepare_hub_search_options,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import default_model_codec

from .._cursor_run import parse_search_cursor
from .._materialize_hits import materialize_search_page
from .._pgroonga_plan import effective_combo_limit
from .._search_count import effective_search_count, resolve_ranked_approximate_total
from ._leg_sql import HubLegSqlContext, build_hub_cte, build_hub_leg_sql_parts
from ._typing_host import HubSearchHost
from .constants import HUB_CTE, HUB_RANK, HUB_ROW_ALIAS, LEG_EID, LEG_SCORE
from .parallel_merge import (
    merge_hub_combo_rows,
    merge_hub_leg_row_lists,
    sort_merged_hub_rows,
)
from .runtime import HubLegRuntime
from .sql import HubSearchSqlMixin, hub_leg_order_limit

M = TypeVar("M", bound=BaseModel)

# ----------------------- #


class HubParallelSearchMixin(HubSearchSqlMixin[M]):
    """Run hub legs as separate ranked queries and merge in Python."""

    async def _hub_parallel_merged_rows(
        self,
        *,
        query_terms: tuple[str, ...],
        filters: QueryFilterExpression | None,
        leg_options: SearchOptions | None,
        member_weights_list: Sequence[float],
        per_leg_limit: int,
        effective_sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> list[dict[str, Any]]:
        host = cast(HubSearchHost[M], self)

        fw, fp = await host.where_clause(filters)
        tenant_id = host._tenant_id_for_resolve()  # type: ignore[protected-access]
        hub_qn = await host._qname()  # type: ignore[protected-access]

        active = [
            (i, leg, float(member_weights_list[i]))
            for i, leg in enumerate(host.members)
            if member_weights_list[i] > 0.0
        ]

        if not query_terms or not active:
            return []

        hub_cols = sql.SQL(", ").join(
            sql.Identifier(HUB_ROW_ALIAS, f) for f in sorted(host.read_fields)
        )
        materialized = bool(getattr(host, "parallel_hub_cte_materialized", True))

        leg_ctx = HubLegSqlContext(
            hub_rel_ident=hub_qn.ident(),
            fw=fw,
            tenant_id=tenant_id,
            query_terms=query_terms,
            leg_options=leg_options,
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
                legs=[leg for _, leg, _ in active],
                weights=weights,
                score_merge=host.score_merge,  # type: ignore[arg-type]
                combine=host.combine,  # type: ignore[arg-type]
                read_fields=host.read_fields,
            )

        else:
            leg_maps = await asyncio.gather(
                *[_fetch_leg_ranked(i, leg) for i, leg, _ in active],
            )
            hub_stmt = sql.SQL(
                """
                WITH {hub_cte}
                SELECT {hub_cols}
                FROM {hf} {ha}
                """
            ).format(
                hub_cte=hub_cte_body,
                hub_cols=hub_cols,
                hf=sql.Identifier(HUB_CTE),
                ha=sql.Identifier(HUB_ROW_ALIAS),
            )
            hub_rows = await host.client.fetch_all(
                hub_stmt, list(fp), row_factory="dict"
            )
            leg_ranked = [
                (leg, m) for (_, leg, _), m in zip(active, leg_maps, strict=True)
            ]
            weights = [w for _, _, w in active]
            merged = merge_hub_combo_rows(
                hub_rows=hub_rows,
                leg_ranked=leg_ranked,
                weights=weights,
                score_merge=host.score_merge,  # type: ignore[arg-type]
                combine=host.combine,  # type: ignore[arg-type]
                read_fields=host.read_fields,
            )

        types = await host.column_types()
        sort_merged_hub_rows(
            merged,
            do_legs=True,
            sorts=effective_sorts,
            read_fields=host.read_fields,
            column_types=types,
            model_type=host.model_type,
            nested_field_hints=host.nested_field_hints,
        )
        return merged

    # ....................... #

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
        _ = members, vector_embedders, score_merge, combine
        host = cast(HubSearchHost[M], self)
        terms = tuple(normalize_search_queries(query))
        leg_options = options
        effective_sorts = sorts if sorts else hub_spec.default_sort

        merged = await self._hub_parallel_merged_rows(
            query_terms=terms,
            filters=filters,
            leg_options=leg_options,
            member_weights_list=member_weights_list,
            per_leg_limit=per_leg_limit,
            effective_sorts=effective_sorts,
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
                total = await self._hub_sql_combo_count(
                    query_terms=terms,
                    filters=filters,
                    leg_options=leg_options,
                    member_weights_list=member_weights_list,
                    per_leg_limit=per_leg_limit,
                    sorts=effective_sorts,
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

    # ....................... #

    async def _hub_parallel_cursor_search(
        self,
        *,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        cursor: CursorPaginationExpression | None,
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
        hub_spec: HubSearchSpec[M],
        member_weights_list: Sequence[float],
    ) -> Any:

        host = cast(HubSearchHost[M], self)
        terms = tuple(normalize_search_queries(query))
        leg_options, _weights = prepare_hub_search_options(hub_spec, options)
        effective_sorts = sorts if sorts else hub_spec.default_sort

        c = dict(cursor or {})
        lim, use_after, use_before = parse_search_cursor(cursor)

        merged = await self._hub_parallel_merged_rows(
            query_terms=terms,
            filters=filters,
            leg_options=leg_options,
            member_weights_list=member_weights_list,
            per_leg_limit=host.per_leg_limit,
            effective_sorts=effective_sorts,
        )

        rs_spec = hub_spec.snapshot
        resolved_combo = effective_combo_limit(
            config_limit=getattr(host, "combo_limit", None),
            per_leg_limit=host.per_leg_limit,
            options=leg_options,
            pagination=dict(cursor or {}),
            snapshot=None,
            result_snapshot=None,
            rs_spec=rs_spec,
        )

        if resolved_combo is not None:
            merged = merged[:resolved_combo]

        do_legs = True
        key_spec = self._hub_cursor_key_spec(do_legs=do_legs, sorts=sorts)
        sort_keys = [k for k, _ in key_spec]
        directions = [d for _, d in key_spec]

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            tk, td, tv = decode_keyset_v1(token)

            if tk != sort_keys or len(td) != len(directions):
                raise exc.internal("Cursor does not match current search sort")

            for i, di in enumerate(directions):
                if (td[i] or "").lower() != di:
                    raise exc.internal("Cursor does not match current search sort")

            cursor_vals = list(tv)

            def _row_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
                return tuple(row_value_for_sort_key(row, k) for k in sort_keys)

            def _cmp_rows(row: dict[str, Any]) -> bool:
                rt = _row_tuple(row)
                for rv, cv, d in zip(rt, cursor_vals, directions, strict=True):
                    if rv == cv:
                        continue
                    if d == "asc":
                        return rv > cv if use_after else rv < cv
                    return rv < cv if use_after else rv > cv
                return False

            merged = [r for r in merged if _cmp_rows(r)]

        has_more = len(merged) > lim
        rows = merged[:lim]

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

        if return_type is not None:
            v = default_model_codec(return_type).decode_mapping_many(rows)
            return CursorPage(
                hits=v, next_cursor=nxt, prev_cursor=prv, has_more=has_more
            )

        if return_fields is not None:
            rj = [{k: r.get(k, None) for k in return_fields} for r in rows]
            return CursorPage(
                hits=rj, next_cursor=nxt, prev_cursor=prv, has_more=has_more
            )

        m = hub_spec.resolved_read_codec.decode_mapping_many(rows)

        return CursorPage(hits=m, next_cursor=nxt, prev_cursor=prv, has_more=has_more)
