"""PGroonga search with projection vs index-heap separation (CTE pipeline)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Mapping, Sequence
from typing import Any, Final, Literal, TypeVar, final, overload

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.query import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    decode_keyset_v1,
    encode_keyset_v1,
    normalize_sorts_with_id,
    row_value_for_sort_key,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchQueryPort,
    SearchSpec,
    effective_phrase_combine,
    normalize_search_queries,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.query.nested import sort_key_expr
from forze_postgres.pagination import build_order_by_sql, build_seek_condition

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ..txmanager import PostgresTxScopeKey
from ._options import search_options_for_simple_adapter
from ._pgroonga_sql import (
    pgroonga_match_clause,
    pgroonga_phrase_match_text,
    pgroonga_score_rank_expr,
)

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

_DEFAULT_JOIN: Final[tuple[tuple[str, str], ...]] = ((ID_FIELD, ID_FIELD),)

_FILTERED_CTE_ALIAS: Final[str] = "f"
_INDEX_ALIAS: Final[str] = "t"
_PROJECTION_ALIAS: Final[str] = "v"
_SCORED_CTE_ALIAS: Final[str] = "s"
_RANK_COLUMN: Final[str] = "_pgroonga_rank"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresPGroongaSearchAdapterV2[M: BaseModel](
    PostgresGateway[M],
    SearchQueryPort[M],
    TxScopedPort,
):
    """PGroonga :class:`SearchQueryPort` using a projection relation and index heap.

    Structured filters (and tenant scope) apply on the **projection** relation
    (:attr:`~PostgresGateway.qname`), typically a view. Full-text matching and
    ``pgroonga_score`` run on the **index heap** (:attr:`index_heap_qname`) so the
    index can live on a base table while rows are shaped by the view.

    Query shape (simplified)::

        WITH filtered AS (
            SELECT v.<join keys> FROM <projection> v WHERE <filters>
        ),
        scored AS (
            SELECT t.<heap cols> AS <proj keys>, <rank>
            FROM <index heap> t
            INNER JOIN filtered f ON <join>
            WHERE <pgroonga match or TRUE>
        )
        SELECT v.<read fields>
        FROM <projection> v
        INNER JOIN scored s ON <join to projection keys>
        ORDER BY s.<rank> DESC NULLS LAST, <user sorts on v>
    """

    spec: SearchSpec[M]
    """Search specification."""

    index_qname: PostgresQualifiedName
    """Index qualified name."""

    index_heap_qname: PostgresQualifiedName
    """Index heap qualified name (relation which index is built on)."""

    join_pairs: Sequence[tuple[str, str]] | None = attrs.field(default=None)
    """Join pairs (projection column, index heap column)."""

    index_field_map: Mapping[str, str] | None = attrs.field(default=None)
    """Index field map (projection column -> index heap column)."""

    pgroonga_score_version: Literal["v1", "v2"] = "v2"
    """
    Which ``pgroonga_score`` form to emit (from :attr:`PostgresSearchConfig.pgroonga_score_version`).

    ``v2``: ``pgroonga_score(tableoid, ctid)``. ``v1``: ``pgroonga_score(heap alias)`` when the heap
    scan does not support the ``v2`` system columns.
    """

    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)
    """Transaction scope."""

    # ....................... #

    @property
    def _safe_join_pairs(self) -> Sequence[tuple[str, str]]:
        return self.join_pairs or _DEFAULT_JOIN

    # ....................... #

    def __attrs_post_init__(self) -> None:
        proj_keys = {pc for pc, _ in self._safe_join_pairs}

        if len(proj_keys) != len(self._safe_join_pairs):
            raise CoreError("join_pairs must use unique projection column names.")

    # ....................... #

    async def _projection_order_by_clause(
        self,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable | None:
        return await self.order_by_clause(sorts, table_alias=_PROJECTION_ALIAS)

    # ....................... #

    async def _pgroonga_match_combined_query(
        self,
        mq: str,
        *,
        options: SearchOptions | None = None,
    ) -> tuple[sql.Composable, list[Any]]:
        if not mq:
            return sql.SQL("TRUE"), []

        return await pgroonga_match_clause(
            search=self.spec,
            index_field_map=self.index_field_map,
            index_qname=self.index_qname,
            introspector=self.introspector,
            index_alias=_INDEX_ALIAS,
            query=mq,
            options=options,
        )

    # ....................... #

    def _filtered_select_list(self) -> sql.Composable:
        parts = [
            sql.SQL("{} AS {}").format(
                sql.Identifier(_PROJECTION_ALIAS, pc),
                sql.Identifier(pc),
            )
            for pc, _ in self._safe_join_pairs
        ]

        return sql.SQL(", ").join(parts)

    # ....................... #

    def _scored_join_on_filtered(self) -> sql.Composable:
        parts = [
            sql.SQL("{} = {}").format(
                sql.Identifier(_INDEX_ALIAS, ic),
                sql.Identifier(_FILTERED_CTE_ALIAS, pc),
            )
            for pc, ic in self._safe_join_pairs
        ]

        return sql.SQL(" AND ").join(parts)

    # ....................... #

    def _outer_join_on_scored(self) -> sql.Composable:
        parts = [
            sql.SQL("{} = {}").format(
                sql.Identifier(_PROJECTION_ALIAS, pc),
                sql.Identifier(_SCORED_CTE_ALIAS, pc),
            )
            for pc, _ in self._safe_join_pairs
        ]

        return sql.SQL(" AND ").join(parts)

    # ....................... #

    def _scored_select_keys_and_rank(
        self,
        *,
        query: str,
    ) -> tuple[sql.Composable, sql.Composable]:
        key_cols = sql.SQL(", ").join(
            sql.SQL("{} AS {}").format(
                sql.Identifier(_INDEX_ALIAS, ic),
                sql.Identifier(pc),
            )
            for pc, ic in self._safe_join_pairs
        )
        rank = pgroonga_score_rank_expr(
            index_alias=_INDEX_ALIAS,
            rank_column=_RANK_COLUMN,
            query=query,
            score_version=self.pgroonga_score_version,
        )
        return key_cols, rank

    # ....................... #

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[M]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[T]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[False] = ...,
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[True] = ...,
    ) -> Page[M]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[True] = ...,
    ) -> Page[T]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[True] = ...,
    ) -> Page[JsonDict]: ...

    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
        return_count: bool = False,
    ) -> (
        CountlessPage[M]
        | CountlessPage[T]
        | CountlessPage[JsonDict]
        | Page[M]
        | Page[T]
        | Page[JsonDict]
    ):
        options = search_options_for_simple_adapter(options)
        fw, fp = await self.where_clause(filters)
        terms = normalize_search_queries(query)
        combine = effective_phrase_combine(options)
        mq = pgroonga_phrase_match_text(terms, combine=combine)

        if not terms:
            extra_ob = await self._projection_order_by_clause(sorts)
            order_parts: list[sql.Composable] = (  # type: ignore[assignment]
                [extra_ob]
                if extra_ob is not None
                else [
                    sql.SQL("{} ASC").format(
                        sql.Identifier(_PROJECTION_ALIAS, sorted(self.read_fields)[0]),
                    ),
                ]
            )
            order_sql = sql.SQL(", ").join(order_parts)
            count_stmt = sql.SQL(
                """
                SELECT COUNT(*) FROM {proj} {pa} WHERE {fw}
                """
            ).format(
                proj=self.source_qname.ident(),
                pa=sql.Identifier(_PROJECTION_ALIAS),
                fw=fw,
            )
            params_base = list(fp)
            total = 0

            if return_count:
                total = int(
                    await self.client.fetch_value(count_stmt, params_base, default=0),
                )
                if total == 0:
                    return page_from_limit_offset(
                        [],
                        pagination or {},
                        total=0,
                    )

            cols = self.return_clause(
                return_type,
                return_fields,
                table_alias=_PROJECTION_ALIAS,
            )
            data_stmt = sql.SQL(
                """
                SELECT {cols} FROM {proj} {pa} WHERE {fw} ORDER BY {order}
                """
            ).format(
                cols=cols,
                proj=self.source_qname.ident(),
                pa=sql.Identifier(_PROJECTION_ALIAS),
                fw=fw,
                order=order_sql,
            )

            params = params_base
            pagination = pagination or {}
            limit = pagination.get("limit")
            offset = pagination.get("offset")

            if limit is not None:
                data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
                params.append(int(limit))

            if offset is not None:
                data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
                params.append(int(offset))

            rows = await self.client.fetch_all(data_stmt, params, row_factory="dict")

            if return_type is not None:
                v = pydantic_validate_many(return_type, rows)

                if return_count:
                    return page_from_limit_offset(v, pagination, total=total)

                return page_from_limit_offset(v, pagination, total=None)

            if return_fields is not None:
                raw = [{k: r.get(k, None) for k in return_fields} for r in rows]

                if return_count:
                    return page_from_limit_offset(raw, pagination, total=total)

                return page_from_limit_offset(raw, pagination, total=None)

            m = pydantic_validate_many(self.model_type, rows)

            if return_count:
                return page_from_limit_offset(m, pagination, total=total)

            return page_from_limit_offset(m, pagination, total=None)

        sw, sp = await self._pgroonga_match_combined_query(mq, options=options)

        key_sel = self._filtered_select_list()
        scored_keys, scored_rank = self._scored_select_keys_and_rank(query=mq)

        filtered_cte = sql.SQL(
            """
            {filtered} AS (
                SELECT {key_sel}
                FROM {proj} {pa}
                WHERE {fw}
            )"""
        ).format(
            filtered=sql.Identifier(_FILTERED_CTE_ALIAS),
            key_sel=key_sel,
            proj=self.source_qname.ident(),
            pa=sql.Identifier(_PROJECTION_ALIAS),
            fw=fw,
        )

        join_sf = self._scored_join_on_filtered()
        scored_cte = sql.SQL(
            """
            ,
            {scored} AS (
                SELECT {scored_keys}, {scored_rank}
                FROM {heap} {ia}
                INNER JOIN {filtered} {fa} ON ({join_sf})
                WHERE {sw}
            )"""
        ).format(
            scored=sql.Identifier(_SCORED_CTE_ALIAS),
            scored_keys=scored_keys,
            scored_rank=scored_rank,
            heap=self.index_heap_qname.ident(),
            ia=sql.Identifier(_INDEX_ALIAS),
            filtered=sql.Identifier(_FILTERED_CTE_ALIAS),
            fa=sql.Identifier(_FILTERED_CTE_ALIAS),
            join_sf=join_sf,
            sw=sw,
        )

        join_vs = self._outer_join_on_scored()
        from_outer = sql.SQL(
            """
            FROM {proj} {pa}
            INNER JOIN {scored} {sa} ON ({join_vs})
            """
        ).format(
            proj=self.source_qname.ident(),
            pa=sql.Identifier(_PROJECTION_ALIAS),
            scored=sql.Identifier(_SCORED_CTE_ALIAS),
            sa=sql.Identifier(_SCORED_CTE_ALIAS),
            join_vs=join_vs,
        )

        order_parts: list[sql.Composable] = [  # type: ignore[no-redef]
            sql.SQL("{} DESC NULLS LAST").format(
                sql.Identifier(_SCORED_CTE_ALIAS, _RANK_COLUMN),
            )
        ]
        extra_ob = await self._projection_order_by_clause(sorts)

        if extra_ob is not None:
            order_parts.append(extra_ob)

        order_sql = sql.SQL(", ").join(order_parts)
        with_clause = sql.SQL("WITH {}{}").format(filtered_cte, scored_cte)

        count_stmt = sql.SQL(
            """
            {with_clause}
            SELECT COUNT(*) {from_outer}
            """
        ).format(with_clause=with_clause, from_outer=from_outer)

        params_count = [*fp, *sp]
        total = 0

        if return_count:
            total = int(
                await self.client.fetch_value(count_stmt, params_count, default=0),
            )
            if total == 0:
                return page_from_limit_offset(
                    [],
                    pagination or {},
                    total=0,
                )

        cols = self.return_clause(
            return_type,
            return_fields,
            table_alias=_PROJECTION_ALIAS,
        )

        data_stmt = sql.SQL(
            """
            {with_clause}
            SELECT {cols} {from_outer}
            ORDER BY {order}
            """
        ).format(
            with_clause=with_clause,
            cols=cols,
            from_outer=from_outer,
            order=order_sql,
        )

        params = [*fp, *sp]
        pagination = pagination or {}
        limit = pagination.get("limit")
        offset = pagination.get("offset")

        if limit is not None:
            data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(int(limit))

        if offset is not None:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(offset))

        rows = await self.client.fetch_all(data_stmt, params, row_factory="dict")

        if return_type is not None:
            v = pydantic_validate_many(return_type, rows)

            if return_count:
                return page_from_limit_offset(v, pagination, total=total)

            return page_from_limit_offset(v, pagination, total=None)

        if return_fields is not None:
            raw = [{k: r.get(k, None) for k in return_fields} for r in rows]

            if return_count:
                return page_from_limit_offset(raw, pagination, total=total)

            return page_from_limit_offset(raw, pagination, total=None)

        m = pydantic_validate_many(self.model_type, rows)

        if return_count:
            return page_from_limit_offset(m, pagination, total=total)

        return page_from_limit_offset(m, pagination, total=None)

    # ....................... #

    @overload
    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
    ) -> CursorPage[M]: ...

    @overload
    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
    ) -> CursorPage[T]: ...

    @overload
    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
    ) -> CursorPage[JsonDict]: ...

    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> CursorPage[M] | CursorPage[T] | CursorPage[JsonDict]:
        """Keyset forward/back over the filter-only (empty query) scan on the projection.

        Full-text search with a non-empty query is not supported; use
        :meth:`search` with limit/offset for ranked PGroonga pages.
        """

        options = search_options_for_simple_adapter(options)
        terms = normalize_search_queries(query)

        if terms:
            raise CoreError(
                "search_with_cursor does not support a non-empty full-text query for "
                "PostgresPGroongaSearchAdapterV2; use search() with limit/offset, or an "
                "empty query for filter-only keyset pagination on the projection.",
            )

        c = dict(cursor or {})

        if c.get("after") and c.get("before"):
            raise CoreError(
                "Cursor pagination: pass at most one of 'after' or 'before'"
            )

        lim: int = 10 if c.get("limit") is None else int(c["limit"])  # type: ignore[arg-type, assignment, call-overload]

        if lim < 1:
            raise CoreError("Cursor pagination 'limit' must be positive")

        use_after = c.get("after") is not None
        use_before = c.get("before") is not None

        if return_fields is not None:
            if sorts is None:
                req = {sorted(self.read_fields)[0], ID_FIELD}

            else:
                req = {k for k, _ in normalize_sorts_with_id(sorts)}

            if not req.issubset(set(return_fields)):
                raise CoreError(
                    "search_with_cursor with return_fields must include all sort and "
                    "tie-breaker columns used for the cursor (see adapter docs).",
                )

        if sorts is None:
            first = sorted(self.read_fields)[0]
            key_spec: list[tuple[str, str]] = [(first, "asc"), (ID_FIELD, "asc")]

        else:
            key_spec = list(normalize_sorts_with_id(sorts))

        sort_keys = [k for k, _ in key_spec]
        directions = [d for _, d in key_spec]

        fw, fp = await self.where_clause(filters)
        types = await self.column_types()

        exprs = [
            sort_key_expr(
                field=k,
                column_types=types,
                model_type=self.model_type,
                nested_field_hints=self.nested_field_hints,
                table_alias=_PROJECTION_ALIAS,
            )
            for k in sort_keys
        ]

        where_fin: sql.Composable = fw
        params: list[Any] = list(fp)

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            tk, td, tv = decode_keyset_v1(token)

            if tk != sort_keys or len(td) != len(directions):
                raise CoreError("Cursor does not match current search sort")

            for i, di in enumerate(directions):
                if (td[i] or "").lower() != di:
                    raise CoreError("Cursor does not match current search sort")

            sk, sp = build_seek_condition(
                exprs,
                directions,
                list(tv),
                "before" if use_before else "after",
            )

            where_fin = sql.SQL("({} AND ({}))").format(fw, sk)
            params = params + sp

        order_sql = build_order_by_sql(exprs, directions, flip=use_before)
        cols = self.return_clause(
            return_type,
            return_fields,
            table_alias=_PROJECTION_ALIAS,
        )
        data_stmt = sql.SQL(
            """
            SELECT {cols} FROM {proj} {pa} WHERE {w} ORDER BY {order}
            """
        ).format(
            cols=cols,
            proj=self.source_qname.ident(),
            pa=sql.Identifier(_PROJECTION_ALIAS),
            w=where_fin,
            order=order_sql,
        )
        data_stmt = sql.SQL("{} LIMIT {}").format(
            data_stmt,
            sql.Placeholder(),
        )
        params.append(lim + 1)

        raw_rows = list(
            await self.client.fetch_all(data_stmt, params, row_factory="dict")
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
            v = pydantic_validate_many(return_type, rows)

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

        m = pydantic_validate_many(self.model_type, rows)

        return CursorPage(
            hits=m,
            next_cursor=nxt,
            prev_cursor=prv,
            has_more=has_more,
        )
