"""Hub search cursor pagination."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence, TypeVar, cast

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.search import SearchCursorPage
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    keyset_page_bounds,
    validate_cursor_token,
)
from forze.application.contracts.search import (
    SearchOptions,
    cursor_return_fields_for_select,
    facet_size_of,
    resolve_facet_fields,
)
from forze.base.primitives import build_projection
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.sql import (
    build_ranked_cursor_order_by_sql,
    build_seek_condition,
)
from forze_postgres.kernel.sql.query.nested import sort_key_expr

from ....kernel.gateways import PostgresGateway
from .._cursor_run import parse_search_cursor
from .._facets import fetch_hub_facets
from .._materialize_hits import decode_search_hits, search_trust_source
from .._offset_run import hydrate_rows_by_id
from ._facets_highlights import attach_hub_highlights
from .constants import COMBO_ALIAS, HUB_RANK
from .parallel import HubParallelSearchMixin
from .plan import build_hub_search_plan

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


class HubSearchCursorMixin[T: BaseModel](HubParallelSearchMixin[T]):
    """Keyset cursor over the hub ``combo`` CTE."""

    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        """Keyset pagination over the hub ``combo`` row (filter-only or ranked legs).

        **Browse (empty query, no sorts):** Orders by ``id`` ascending when the read
        model defines that field (same as explicit ``sorts`` with ``id`` ascending and
        :meth:`search`). Without an ``id`` field, falls back to the lexicographically
        first read-model field only (ties may be unstable).

        **Ranked:** With active legs and a non-empty query, ordering is merged
        ``_hub_rank`` DESC NULLS LAST, optional ``sorts`` (including ``id`` if given),
        then an ``id`` tie-breaker when omitted.

        With ``return_fields``, list only the columns you want in each hit; keyset
        columns (including ``_hub_rank`` when legs are active) are selected
        internally and stripped from the response.
        """

        plan = await build_hub_search_plan(
            self._hub_host,
            query=query,
            options=options,
            sorts=sorts,
            pagination_or_cursor=dict(cursor or {}),
            snapshot=None,
            result_snapshot=None,
            mode="cursor",
        )

        if plan.use_parallel:
            # Parallel execution merges per-leg results in Python; facets reuse the ``sql``
            # companion GROUP BY (identical distribution), highlights are marked on the hits.
            facet_fields = resolve_facet_fields(self._hub_host.hub_spec, options)
            parallel_page = await self._hub_parallel_cursor_search(
                plan=plan,
                filters=filters,
                cursor=cursor,
                return_type=return_type,
                return_fields=return_fields,
                hub_spec=self._hub_host.hub_spec,
            )
            if facet_fields:
                parallel_facets = await self._hub_parallel_facets(
                    plan,
                    filters=filters,
                    combo_limit=plan.resolved_combo if plan.terms else None,
                    fields=facet_fields,
                    size=facet_size_of(options),
                )
                parallel_page = attrs.evolve(parallel_page, facets=parallel_facets)
            return attach_hub_highlights(
                parallel_page,
                hub_spec=self._hub_host.hub_spec,
                query=query,
                options=options,
                return_fields=return_fields,
            )

        # ``sql`` execution: facets via a companion over the merged set; highlights marked
        # on the returned page (field validation runs in both helpers).
        facet_fields = resolve_facet_fields(self._hub_host.hub_spec, options)

        c = dict(cursor or {})
        lim, use_after, use_before = parse_search_cursor(cursor)

        combo_cap = plan.resolved_combo if plan.terms else None

        # Late materialization: keyset-paginate over the thin id pipeline and
        # hydrate the heavy read-model columns for the returned page by id. Cursor search
        # never writes a result snapshot, so it is always eligible when the shape is thinnable.
        thin_fields = self._hub_thin_projection(plan)
        thin = thin_fields is not None

        with_clause, params, do_legs, count_rel, data_relation = (
            await self._hub_build_with_clause_from_plan(
                plan,
                filters=filters,
                combo_limit=combo_cap,
                thin=thin,
            )
        )

        # Facets count over the full merged set, so capture the WITH-clause params before the
        # seek/limit params are appended below; the companion runs without the keyset bound.
        facets = (
            await fetch_hub_facets(
                self._hub_host.client,
                with_clause=with_clause,
                count_relation=count_rel,
                combo_alias=COMBO_ALIAS,
                read_relation=await self._hub_host._qname(),  # pyright: ignore[reportPrivateUsage]
                params=list(params),
                fields=facet_fields,
                size=facet_size_of(options),
            )
            if facet_fields
            else None
        )

        sort_keys = [k for k, _ in plan.order_key_spec]
        directions = [d for _, d in plan.order_key_spec]

        types = await self._hub_host.column_types()
        exprs: list[sql.Composable] = []

        for k in sort_keys:
            if k == HUB_RANK:
                exprs.append(sql.Identifier(COMBO_ALIAS, HUB_RANK))
            else:
                exprs.append(
                    sort_key_expr(
                        field=k,
                        column_types=types,
                        model_type=self._hub_host.model_type,
                        nested_field_hints=self._hub_host.nested_field_hints,
                        table_alias=COMBO_ALIAS,
                    ),
                )

        where_fin: sql.Composable = sql.SQL("TRUE")

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            tv = validate_cursor_token(
                token,
                sort_keys=sort_keys,
                directions=directions,
            )

            sk, sp = build_seek_condition(
                exprs,
                directions,
                tv,
                "before" if use_before else "after",
            )

            where_fin = sk
            params = params + sp

        order_sql = build_ranked_cursor_order_by_sql(
            exprs,
            sort_keys,
            directions,
            rank_key=HUB_RANK,
            flip=use_before,
        )

        return_fields_sql: Sequence[str] | None

        if return_fields is not None:
            return_fields_sql = cursor_return_fields_for_select(
                sort_keys=sort_keys,
                rank_field=HUB_RANK if do_legs else None,
                return_fields=return_fields,
            )
            if not return_fields_sql:
                return_fields_sql = None
        else:
            return_fields_sql = None

        cols: sql.Composable

        if (
            thin
            and thin_fields is not None  # pyright: ignore[reportUnnecessaryComparison]
        ):
            # Phase A selects only key/sort columns (+ rank); heavy columns are hydrated
            # for the page after the keyset bounds are computed.
            cols = sql.SQL(", ").join(
                sql.Identifier(COMBO_ALIAS, f) for f in thin_fields
            )
        else:
            cols = self._hub_host.return_clause(
                return_type,
                return_fields_sql,
                table_alias=COMBO_ALIAS,
            )

        if do_legs:
            cols = sql.SQL("{}, {}").format(
                cols,
                sql.SQL("{} AS {}").format(
                    sql.SQL("{}.{}").format(
                        sql.Identifier(COMBO_ALIAS),
                        sql.Identifier(HUB_RANK),
                    ),
                    sql.Identifier(HUB_RANK),
                ),
            )

        data_stmt = sql.SQL(
            """
            {with_clause}
            SELECT {cols} FROM {combo} {ca}
            WHERE {w}
            ORDER BY {order}
            """
        ).format(
            with_clause=with_clause,
            cols=cols,
            combo=sql.Identifier(data_relation),
            ca=sql.Identifier(COMBO_ALIAS),
            w=where_fin,
            order=order_sql,
        )
        data_stmt = sql.SQL("{} LIMIT {}").format(
            data_stmt,
            sql.Placeholder(),
        )
        params.append(lim + 1)

        raw_rows = list(
            await self._hub_host.client.fetch_all(
                data_stmt, params, row_factory="dict"
            ),
        )  # type: ignore[assignment, arg-type]

        rows, has_more, nxt, prv = keyset_page_bounds(
            raw_rows,
            lim,
            sort_keys=sort_keys,
            directions=directions,
            use_after=use_after,
            use_before=use_before,
        )

        # The page rows still carry the hub rank (selected for keyset when do_legs); surface it
        # as the per-hit score before hydration/projection drops it. Browse has no score.
        scores = [float(r[HUB_RANK]) for r in rows] if do_legs else None

        trust = search_trust_source(self._hub_host.read_validation)

        # Phase B: the keyset bounds (and next/prev cursors) were computed from the thin
        # sort-key values above; hydrate the heavy read-model columns for the page only.
        source_rows = rows

        if thin:
            page_ids = [r[ID_FIELD] for r in rows]
            source_rows = await hydrate_rows_by_id(
                cast(PostgresGateway[Any], self._hub_host),
                page_ids=page_ids,
                return_type=return_type,
                return_fields=return_fields,
            )

        if return_fields is not None:
            rj = [build_projection(r, return_fields) for r in source_rows]

            return attach_hub_highlights(
                SearchCursorPage(
                    hits=rj,
                    next_cursor=nxt,
                    prev_cursor=prv,
                    has_more=has_more,
                    facets=facets,
                    scores=scores,
                ),
                hub_spec=self._hub_host.hub_spec,
                query=query,
                options=options,
                return_fields=return_fields,
            )

        hits = decode_search_hits(
            rows=source_rows,
            model_type=self._hub_host.model_type,
            codec=self._hub_host.hub_spec.resolved_read_codec,
            return_type=return_type,
            trust_source=trust,
        )

        return attach_hub_highlights(
            SearchCursorPage(
                hits=hits,
                next_cursor=nxt,
                prev_cursor=prv,
                has_more=has_more,
                facets=facets,
                scores=scores,
            ),
            hub_spec=self._hub_host.hub_spec,
            query=query,
            options=options,
        )
