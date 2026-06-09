"""Hub search cursor pagination."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence, TypeVar

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.base import CursorPage
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
)

from .._cursor_run import parse_search_cursor
from .._materialize_hits import decode_search_hits, search_trust_source
from forze_postgres.kernel.sql import build_ranked_cursor_order_by_sql, build_seek_condition
from forze_postgres.kernel.sql.query.nested import sort_key_expr

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
            return await self._hub_parallel_cursor_search(
                plan=plan,
                filters=filters,
                cursor=cursor,
                return_type=return_type,
                return_fields=return_fields,
                hub_spec=self._hub_host.hub_spec,
            )

        c = dict(cursor or {})
        lim, use_after, use_before = parse_search_cursor(cursor)

        combo_cap = plan.resolved_combo if plan.terms else None

        with_clause, params, do_legs, _count_rel, data_relation = (
            await self._hub_build_with_clause_from_plan(
                plan,
                filters=filters,
                combo_limit=combo_cap,
            )
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

        base_cols = self._hub_host.return_clause(
            return_type,
            return_fields_sql,
            table_alias=COMBO_ALIAS,
        )

        cols: sql.Composable

        if do_legs:
            cols = sql.SQL("{}, {}").format(
                base_cols,
                sql.SQL("{} AS {}").format(
                    sql.SQL("{}.{}").format(
                        sql.Identifier(COMBO_ALIAS),
                        sql.Identifier(HUB_RANK),
                    ),
                    sql.Identifier(HUB_RANK),
                ),
            )

        else:
            cols = base_cols

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

        trust = search_trust_source(self._hub_host.read_validation)

        if return_fields is not None:
            rj = [{k: r.get(k, None) for k in return_fields} for r in rows]

            return CursorPage(
                hits=rj,
                next_cursor=nxt,
                prev_cursor=prv,
                has_more=has_more,
            )

        hits = decode_search_hits(
            rows=rows,
            model_type=self._hub_host.model_type,
            codec=self._hub_host.hub_spec.resolved_read_codec,
            return_type=return_type,
            trust_source=trust,
        )

        return CursorPage(
            hits=hits,
            next_cursor=nxt,
            prev_cursor=prv,
            has_more=has_more,
        )
