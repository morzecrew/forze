"""Cursor pagination execution for Postgres ranked pipeline search."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.search import (
    HitHighlights,
    SearchCursorPage,
)
from forze.application.contracts.querying import (
    CursorBinding,
    CursorPaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    build_cursor_binding,
    cursor_protection_active,
    keyset_page_bounds,
    normalize_sorts_for_keyset,
    resolve_effective_sorts,
    resolved_cursor_limit,
    validate_cursor_token,
)
from forze.application.contracts.search import (
    SearchSpec,
    cursor_return_fields_for_select,
    ranked_search_cursor_key_spec,
)
from forze.application.integrations.search import decrypt_search_rows
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, build_projection
from forze.base.serialization import ModelCodec

from ._highlights import extract_and_strip_highlights
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
        raise exc.validation("Cursor pagination: pass at most one of 'after' or 'before'")

    # Shared with document pagination: a non-integer ``limit`` is a clean 400 (not a raw
    # ``ValueError``) and an over-large value is clamped to ``MAX_CURSOR_LIMIT`` rather than
    # reaching the backend as an unbounded fetch.
    lim = resolved_cursor_limit(c)

    return lim, c.get("after") is not None, c.get("before") is not None


# ....................... #


_UNSET: Any = object()


def _search_cursor_binding(
    gw: PostgresGateway[Any],
    spec_name: str,
    *,
    filters: Any,
    parsed: Any = _UNSET,
) -> CursorBinding | None:
    """Bind a search cursor to its (spec, tenant, filter) — only while protection is active.

    Returns ``None`` when neither a signer nor a cipher is bound (the embedded binding is
    authenticated, so it is only meaningful then), which also skips parsing the filter on the
    unprotected hot path. A caller with the filter already parsed passes it as *parsed*.
    """

    if not cursor_protection_active():
        return None

    expr = parsed if parsed is not _UNSET else gw.compile_filters(filters)

    return build_cursor_binding(
        spec_name=spec_name,
        tenant_id=gw.require_tenant_if_aware(),
        filter_expr=expr,
    )


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
) -> SearchCursorPage[Any]:
    """Keyset cursor on the projection relation only (empty search query)."""

    lim, use_after, use_before = parse_search_cursor(cursor)
    c = dict(cursor or {})
    proj_qn = await gw._qname()  # pyright: ignore[reportPrivateUsage]

    effective = resolve_effective_sorts(
        sorts=sorts,
        default_sort=spec.default_sort,
        read_fields=gw.read_fields,
        spec_name=spec.name,
        model=gw.model_type,
    )
    key_spec = [
        (k, d)
        for k, d, _ in normalize_sorts_for_keyset(
            effective,
            read_fields=gw.read_fields,
            model=gw.model_type,
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

    binding = _search_cursor_binding(
        gw, spec.name, filters=filters, parsed=parsed_filters
    )

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
            binding=binding,
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
        binding=binding,
    )

    return await _cursor_page_from_rows(
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
) -> SearchCursorPage[Any]:
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
    binding = _search_cursor_binding(gw, spec.name, filters=filters)
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

    # Highlight column placeholders sit in the SELECT list, between the WITH-clause params
    # and any from_outer params; splice them at that boundary (mirrors the offset path).
    hl = pipeline_sql.highlight
    body = list(pipeline_sql.params_body)

    if hl is not None:
        split = len(body) - pipeline_sql.from_outer_param_count
        body = [*body[:split], *hl.params, *body[split:]]

    params: list[Any] = body

    if use_after or use_before:
        token = str(c["after" if use_after else "before"])
        tv = validate_cursor_token(
            token,
            sort_keys=sort_keys,
            directions=directions,
            binding=binding,
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

    cols = sql.SQL("{}, {}{}").format(
        base_cols,
        sql.SQL("{} AS {}").format(
            sql.Identifier(pipe.scored, rank_col),
            sql.Identifier(rank_col),
        ),
        hl.select_fragment() if hl is not None else sql.SQL(""),
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
        binding=binding,
    )

    # Capture + strip the synthetic highlight columns from the final page rows (after
    # keyset slicing, so they stay aligned with the returned hits).
    highlights = extract_and_strip_highlights(rows, hl) if hl is not None else None

    # This path only runs for a ranked (non-browse) query, so every row carries the rank
    # column; surface it as the per-hit score (decode ignores the extra key).
    scores = [float(r[rank_col]) for r in rows]

    return await _cursor_page_from_rows(
        rows,
        return_type=return_type,
        return_fields=return_fields,
        model_type=gw.model_type,
        codec=spec.resolved_read_codec,
        next_cursor=nxt,
        prev_cursor=prv,
        has_more=has_more,
        trust_source=trust_source,
        highlights=highlights,
        scores=scores,
    )


# ....................... #


async def _cursor_page_from_rows(
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
    highlights: list[HitHighlights] | None = None,
    scores: list[float] | None = None,
) -> SearchCursorPage[Any]:
    # Decrypt sealed fields out of the raw rows once, so the spec model, a custom
    # return_type, and raw field projections all read plaintext (no-op for a plain codec).
    rows, codec = await decrypt_search_rows(codec, rows)

    hits: list[Any]

    if return_fields is not None:
        hits = [build_projection(r, return_fields) for r in rows]

    else:
        hits = decode_search_hits(
            rows=rows,
            model_type=model_type,
            codec=codec,
            return_type=return_type,
            trust_source=trust_source,
        )

    return SearchCursorPage(
        hits=hits,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
        has_more=has_more,
        highlights=highlights,
        scores=scores,
    )
