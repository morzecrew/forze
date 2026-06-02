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
    decode_keyset_v1,
    encode_keyset_v1,
    row_value_for_sort_key,
)
from forze.application.contracts.search import (
    SearchOptions,
    cursor_return_fields_for_select,
    normalize_search_queries,
    prepare_hub_search_options,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import default_model_codec
from forze_postgres.kernel.sql import build_seek_condition
from forze_postgres.kernel.sql.query.nested import sort_key_expr

from .._cursor_run import parse_search_cursor
from .constants import COMBO_ALIAS, HUB_RANK
from .sql import HubSearchSqlMixin

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


class HubSearchCursorMixin[M: BaseModel](HubSearchSqlMixin[M]):
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

        terms = normalize_search_queries(query)

        leg_options, member_weights_list = prepare_hub_search_options(
            self._hub_host.hub_spec,
            options,
        )

        c = dict(cursor or {})
        lim, use_after, use_before = parse_search_cursor(cursor)

        with_clause, params, do_legs = await self._hub_build_with_clause(
            query_terms=terms,
            filters=filters,
            leg_options=leg_options,
            member_weights_list=member_weights_list,
            per_leg_limit=self._hub_host.per_leg_limit,
        )

        key_spec = self._hub_cursor_key_spec(do_legs=do_legs, sorts=sorts)
        sort_keys = [k for k, _ in key_spec]
        directions = [d for _, d in key_spec]

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
            tk, td, tv = decode_keyset_v1(token)

            if tk != sort_keys or len(td) != len(directions):
                raise exc.internal("Cursor does not match current search sort")

            for i, di in enumerate(directions):
                if (td[i] or "").lower() != di:
                    raise exc.internal("Cursor does not match current search sort")

            sk, sp = build_seek_condition(
                exprs,
                directions,
                list(tv),
                "before" if use_before else "after",
            )

            where_fin = sk
            params = params + sp

        order_sql = self._hub_cursor_order_sql(
            exprs,
            sort_keys,
            directions,
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
            combo=sql.Identifier("combo"),
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

        if use_before:
            raw_rows = list(reversed(raw_rows))

        has_more = len(raw_rows) > lim
        rows = raw_rows[:lim]

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
                hits=v,
                next_cursor=nxt,
                prev_cursor=prv,
                has_more=has_more,
            )
        if return_fields is not None:
            rj = [{k: r.get(k, None) for k in return_fields} for r in rows]

            return CursorPage(
                hits=rj,
                next_cursor=nxt,
                prev_cursor=prv,
                has_more=has_more,
            )

        m = self._hub_host.hub_spec.resolved_read_codec.decode_mapping_many(rows)

        return CursorPage(
            hits=m,
            next_cursor=nxt,
            prev_cursor=prv,
            has_more=has_more,
        )
