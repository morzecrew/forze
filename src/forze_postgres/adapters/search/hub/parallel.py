"""Parallel per-leg hub search execution."""

from __future__ import annotations

from functools import partial
from typing import Any, Sequence, TypeVar, cast

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.search import (
    FacetResults,
    SearchCursorPage,
    search_page_from_limit_offset,
)
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    build_cursor_binding,
    cursor_protection_active,
    keyset_page_bounds,
    row_passes_keyset_seek,
    validate_cursor_token,
)
from forze.application.contracts.search import SearchOptions, SearchResultSnapshotOptions
from forze.application.integrations.search import (
    SearchResultSnapshot,
    decrypt_search_rows,
)
from forze.base.primitives import build_projection
from forze.domain.constants import ID_FIELD

from ....kernel.client import gather_db_work
from ....kernel.gateways import PostgresGateway
from .._cursor_run import parse_search_cursor
from .._facets import fetch_hub_facets
from .._materialize_hits import decode_search_hits, materialize_search_page, search_trust_source
from .._offset_run import hydrate_rows_by_id
from .._search_count import resolve_ranked_approximate_total
from ._leg_sql import HubLegSqlContext, build_hub_cte, build_hub_leg_sql_parts
from ._typing_host import HubSearchHost
from .constants import COMBO_ALIAS, HUB_CTE, HUB_RANK, HUB_ROW_ALIAS, LEG_EID, LEG_SCORE
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

        # Late materialization: fetch only the key/sort columns (id + leg FK + sort-key roots)
        # through the per-leg join / union so the Python merge holds thin rows, then hydrate the
        # heavy read-model columns for the final page by id. Falls back to the full read-field
        # projection when the shape can't be thinned (no id column, non-projectable sort key).
        thin_fields = self._hub_thin_projection(plan)
        select_fields = thin_fields if thin_fields is not None else sorted(host.read_fields)

        hub_cols = sql.SQL(", ").join(
            sql.Identifier(HUB_ROW_ALIAS, f) for f in select_fields
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

            leg_row_lists = await gather_db_work(
                host.client,
                [partial(_run_leg_joined, i, leg) for i, leg, _ in active],
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
            leg_maps = await gather_db_work(
                host.client,
                [partial(_fetch_leg_ranked, i, leg) for i, leg, _ in active],
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

    async def _hub_parallel_page_rows(
        self,
        plan: HubSearchPlan,
        page_rows: Sequence[dict[str, Any]],
        *,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
    ) -> list[dict[str, Any]]:
        """Full read-model rows for the page. Phase B of late materialization.

        When the merge ran over thin rows (see :meth:`_hub_thin_projection`), the page's rows
        carry only id/sort/key columns, so hydrate the heavy read-model columns by primary key
        from the hub relation; otherwise the merged rows already hold every read field.
        """

        if self._hub_thin_projection(plan) is None:
            return [hub_row_for_materialize(r) for r in page_rows]

        return await hydrate_rows_by_id(
            cast(PostgresGateway[Any], self),
            page_ids=[r[ID_FIELD] for r in page_rows],
            return_type=return_type,
            return_fields=return_fields,
        )

    # ....................... #

    async def _hub_parallel_facets(
        self,
        plan: HubSearchPlan,
        *,
        filters: QueryFilterExpression | None,
        combo_limit: int | None,
        fields: Sequence[str],
        size: int,
    ) -> FacetResults:
        """Term facets for parallel execution via the same companion the ``sql`` path runs.

        The Python merge operates on thin candidate rows that don't carry the facet value
        column, so faceting reuses :func:`fetch_hub_facets` over an id-only ``WITH`` pipeline
        joined to the read relation — an independent query whose distribution is byte-for-byte
        the ``sql`` path's, keeping facets identical across execution modes.
        """

        host = cast(HubSearchHost[M], self)
        with_clause, params, _, count_relation, _ = (
            await self._hub_build_with_clause_from_plan(
                plan,
                filters=filters,
                combo_limit=combo_limit,
                thin=True,
            )
        )

        return await fetch_hub_facets(
            host.client,
            with_clause=with_clause,
            count_relation=count_relation,
            combo_alias=COMBO_ALIAS,
            read_relation=await host._qname(),  # type: ignore[protected-access]
            params=list(params),
            fields=fields,
            size=size,
        )

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

        page_slice = merged[offset : offset + page_limit]
        # The merged rows carry the hub score (``_hub_rank``); capture it before hydration
        # strips it. A filter-only browse (``not do_legs``) has no meaningful score.
        scores = (
            [float(r[HUB_RANK]) for r in page_slice] if plan.do_legs else None
        )
        page_rows = await self._hub_parallel_page_rows(
            plan,
            page_slice,
            return_type=return_type,
            return_fields=return_fields,
        )
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
                scores=scores,
            )

        return search_page_from_limit_offset(page, pagination_dict, total=None, scores=scores)

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

        binding = (
            build_cursor_binding(
                spec_name=hub_spec.name,
                tenant_id=host._tenant_id_for_resolve(),  # type: ignore[protected-access]
                filter_expr=host.compile_filters(filters),
            )
            if cursor_protection_active()
            else None
        )

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            cursor_vals = validate_cursor_token(
                token,
                sort_keys=sort_keys,
                directions=directions,
                binding=binding,
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

        # Feed the shared page-boundary helper the same shape a flipped backend fetch would
        # produce: a ``before`` page walks the merged (final-order) rows from the cursor
        # backwards, so its over-fetch window is the last ``lim + 1`` rows, reversed —
        # descending from the cursor with the sentinel (the farthest row) last. Only that
        # tail is copied and flipped; feeding the plain ascending tail instead would return
        # the page in descending order and mint the next/prev cursors from each other's rows.
        if use_before:
            window = merged[-(lim + 1) :]
            window.reverse()
        else:
            window = merged[: lim + 1]

        rows, has_more, nxt, prv = keyset_page_bounds(
            window,
            lim,
            sort_keys=sort_keys,
            directions=directions,
            use_after=use_after,
            use_before=use_before,
            binding=binding,
        )

        # Capture the hub score off the page rows before hydration strips ``_hub_rank``
        # (aligned with the final hit order; browse has no score).
        scores = [float(r[HUB_RANK]) for r in rows] if plan.do_legs else None

        trust = search_trust_source(host.read_validation)
        # Phase B: keyset bounds / cursors were computed from the thin sort-key values above;
        # hydrate the heavy read-model columns for the returned page only (by id when thinned).
        source_rows = await self._hub_parallel_page_rows(
            plan, rows, return_type=return_type, return_fields=return_fields
        )
        # Decrypt sealed hub-row fields once, so a raw field projection and a decoded
        # return_type both read plaintext (no-op if plaintext).
        mat_rows, decode_codec = await decrypt_search_rows(
            hub_spec.resolved_read_codec, source_rows
        )

        if return_fields is not None:
            rj = [build_projection(r, return_fields) for r in mat_rows]
            return SearchCursorPage(
                hits=rj,
                next_cursor=nxt,
                prev_cursor=prv,
                has_more=has_more,
                scores=scores,
            )

        hits = decode_search_hits(
            rows=mat_rows,
            model_type=host.model_type,
            codec=decode_codec,
            return_type=return_type,
            trust_source=trust,
        )

        return SearchCursorPage(
            hits=hits,
            next_cursor=nxt,
            prev_cursor=prv,
            has_more=has_more,
            scores=scores,
        )
