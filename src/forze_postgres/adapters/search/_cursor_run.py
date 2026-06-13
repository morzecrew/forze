"""Cursor pagination execution for Postgres ranked pipeline search."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.base import CursorPage
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    keyset_page_bounds,
    normalize_sorts_for_keyset,
    resolve_effective_sorts,
    validate_cursor_token,
)
from forze.application.contracts.search import (
    SearchSpec,
    cursor_return_fields_for_select,
    ranked_search_cursor_key_spec,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec

from ._materialize_hits import decode_search_hits
from forze_postgres.kernel.sql import (
    build_order_by_sql,
    build_ranked_cursor_order_by_sql,
    build_seek_condition,
)
from forze_postgres.kernel.sql.query.nested import sort_key_expr

from ...kernel.gateways import PostgresGateway
from ._engine import RankedPipelineSql

# ----------------------- #


def parse_search_cursor(
    cursor: CursorPaginationExpression | None,
) -> tuple[int, bool, bool]:
    """Return ``(limit, use_after, use_before)`` from a cursor expression."""

    c = dict(cursor or {})

    if c.get("after") and c.get("before"):
        raise exc.internal("Cursor pagination: pass at most one of 'after' or 'before'")

    lim: int = 10 if c.get("limit") is None else int(c["limit"])  # type: ignore[arg-type, assignment, call-overload]

    if lim < 1:
        raise exc.internal("Cursor pagination 'limit' must be positive")

    return lim, c.get("after") is not None, c.get("before") is not None


# ....................... #


async def execute_projection_keyset_cursor[M: BaseModel](
    gw: PostgresGateway[M],
    *,
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    cursor: CursorPaginationExpression | None,
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    spec: SearchSpec[Any],
    projection_alias: str,
    parsed_filters: Any,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    trust_source: bool = False,
) -> CursorPage[Any]:
    """Keyset cursor on the projection relation only (empty search query)."""

    lim, use_after, use_before = parse_search_cursor(cursor)
    c = dict(cursor or {})
    proj_qn = await gw._qname()  # pyright: ignore[reportPrivateUsage]

    effective = resolve_effective_sorts(
        sorts=sorts,
        default_sort=spec.default_sort,
        read_fields=gw.read_fields,
        spec_name=spec.name,
    )
    key_spec = [
        (k, d)
        for k, d, _ in normalize_sorts_for_keyset(
            effective,
            read_fields=gw.read_fields,
        )
    ]

    sort_keys = [k for k, _ in key_spec]
    directions = [d for _, d in key_spec]

    if return_fields is not None:
        select_rf = cursor_return_fields_for_select(
            sort_keys=sort_keys,
            rank_field=None,
            return_fields=return_fields,
        )

    else:
        select_rf = None

    fw, fp = await gw.where_clause(filters, parsed=parsed_filters)
    types = await gw.column_types()

    exprs = [
        sort_key_expr(
            field=k,
            column_types=types,
            model_type=gw.model_type,
            nested_field_hints=gw.nested_field_hints,
            table_alias=projection_alias,
        )
        for k in sort_keys
    ]

    where_fin: sql.Composable = fw
    params: list[Any] = list(fp)

    if use_after or use_before:
        token = str(c["after" if use_after else "before"])
        tv = validate_cursor_token(
            token,
            sort_keys=sort_keys,
            directions=directions,
        )

        sk, sp_seek = build_seek_condition(
            exprs,
            directions,
            tv,
            "before" if use_before else "after",
        )

        where_fin = sql.SQL("({} AND ({}))").format(fw, sk)
        params = params + sp_seek

    order_sql = build_order_by_sql(exprs, directions, flip=use_before)
    cols = gw.return_clause(
        return_type,
        select_rf,
        table_alias=projection_alias,
    )
    data_stmt = sql.SQL(
        """
        SELECT {cols} FROM {proj} {pa} WHERE {w} ORDER BY {order}
        """
    ).format(
        cols=cols,
        proj=proj_qn.ident(),
        pa=sql.Identifier(projection_alias),
        w=where_fin,
        order=order_sql,
    )
    data_stmt = sql.SQL("{} LIMIT {}").format(
        data_stmt,
        sql.Placeholder(),
    )
    params.append(lim + 1)

    raw_rows = list(
        await gw.client.fetch_all(data_stmt, params, row_factory="dict")
    )  # type: ignore[assignment, arg-type]

    rows, has_more, nxt, prv = keyset_page_bounds(
        raw_rows,
        lim,
        sort_keys=sort_keys,
        directions=directions,
        use_after=use_after,
        use_before=use_before,
    )

    return _cursor_page_from_rows(
        rows,
        return_type=return_type,
        return_fields=return_fields,
        model_type=gw.model_type,
        codec=spec.resolved_read_codec,
        next_cursor=nxt,
        prev_cursor=prv,
        has_more=has_more,
        trust_source=trust_source,
    )


# ....................... #


async def execute_ranked_pipeline_cursor[M: BaseModel](
    gw: PostgresGateway[M],
    *,
    pipeline_sql: RankedPipelineSql,
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    cursor: CursorPaginationExpression | None,
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    spec: SearchSpec[Any],
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    trust_source: bool = False,
) -> CursorPage[Any]:
    """Keyset cursor over a ranked projection + index-heap pipeline."""

    lim, use_after, use_before = parse_search_cursor(cursor)
    c = dict(cursor or {})

    pipe = pipeline_sql.pipeline
    rank_col = pipeline_sql.rank_column
    proj_alias = pipeline_sql.projection_alias

    user_sorts = sorts if sorts else spec.default_sort

    key_spec = ranked_search_cursor_key_spec(
        rank_field=rank_col,
        sorts=user_sorts,
        read_fields=gw.read_fields,
    )
    sort_keys = [k for k, _ in key_spec]
    directions = [d for _, d in key_spec]

    types = await gw.column_types()
    exprs: list[sql.Composable] = []

    for k in sort_keys:
        if k == rank_col:
            exprs.append(sql.Identifier(pipe.scored, rank_col))

        else:
            exprs.append(
                sort_key_expr(
                    field=k,
                    column_types=types,
                    model_type=gw.model_type,
                    nested_field_hints=gw.nested_field_hints,
                    table_alias=proj_alias,
                )
            )

    where_fin: sql.Composable = sql.SQL("TRUE")
    params: list[Any] = list(pipeline_sql.params_body)

    if use_after or use_before:
        token = str(c["after" if use_after else "before"])
        tv = validate_cursor_token(
            token,
            sort_keys=sort_keys,
            directions=directions,
        )

        sk, sp_seek = build_seek_condition(
            exprs,
            directions,
            tv,
            "before" if use_before else "after",
        )

        where_fin = sk
        params = params + sp_seek

    order_sql = build_ranked_cursor_order_by_sql(
        exprs,
        sort_keys,
        directions,
        rank_key=rank_col,
        flip=use_before,
    )

    return_fields_sql: Sequence[str] | None

    if return_fields is not None:
        selected = cursor_return_fields_for_select(
            sort_keys=sort_keys,
            rank_field=rank_col,
            return_fields=return_fields,
        )
        return_fields_sql = selected if selected else None

    else:
        return_fields_sql = None

    base_cols = gw.return_clause(
        return_type,
        return_fields_sql,
        table_alias=proj_alias,
    )

    cols = sql.SQL("{}, {}").format(
        base_cols,
        sql.SQL("{} AS {}").format(
            sql.Identifier(pipe.scored, rank_col),
            sql.Identifier(rank_col),
        ),
    )

    data_stmt = sql.SQL(
        """
        {with_clause}
        SELECT {cols} {from_outer}
        WHERE {w}
        ORDER BY {order}
        """
    ).format(
        with_clause=pipeline_sql.with_clause,
        cols=cols,
        from_outer=pipeline_sql.from_outer,
        w=where_fin,
        order=order_sql,
    )

    data_stmt = sql.SQL("{} LIMIT {}").format(
        data_stmt,
        sql.Placeholder(),
    )
    params.append(lim + 1)

    raw_rows = list(
        await gw.client.fetch_all(data_stmt, params, row_factory="dict")
    )  # type: ignore[assignment, arg-type]

    rows, has_more, nxt, prv = keyset_page_bounds(
        raw_rows,
        lim,
        sort_keys=sort_keys,
        directions=directions,
        use_after=use_after,
        use_before=use_before,
    )

    return _cursor_page_from_rows(
        rows,
        return_type=return_type,
        return_fields=return_fields,
        model_type=gw.model_type,
        codec=spec.resolved_read_codec,
        next_cursor=nxt,
        prev_cursor=prv,
        has_more=has_more,
        trust_source=trust_source,
    )


# ....................... #


def _cursor_page_from_rows(
    rows: list[JsonDict],
    *,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    model_type: type[BaseModel],
    codec: ModelCodec[Any, Any],
    next_cursor: str | None,
    prev_cursor: str | None,
    has_more: bool,
    trust_source: bool = False,
) -> CursorPage[Any]:
    hits: list[Any]

    if return_fields is not None:
        hits = [{k: r.get(k, None) for k in return_fields} for r in rows]

    else:
        hits = decode_search_hits(
            rows=rows,
            model_type=model_type,
            codec=codec,
            return_type=return_type,
            trust_source=trust_source,
        )

    return CursorPage(
        hits=hits,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
        has_more=has_more,
    )
