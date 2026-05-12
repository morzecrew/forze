"""FTS search with projection vs index-heap separation (CTE pipeline)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Final, Literal, Mapping, Sequence, TypeVar, final, overload

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
    SearchResultSnapshotOptions,
    SearchSpec,
    cursor_return_fields_for_select,
    effective_phrase_combine,
    normalize_search_queries,
    ranked_search_cursor_key_spec,
    search_options_for_simple_adapter,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.application.coordinators import SearchResultSnapshotCoordinator
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.query.nested import sort_key_expr
from forze_postgres.pagination import (
    build_order_by_sql,
    build_ranked_cursor_order_by_sql,
    build_seek_condition,
)

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ..txmanager import PostgresTxScopeKey
from ._fts_sql import (
    FtsGroupLetter,
    fts_effective_group_weights,
    fts_match_predicate,
    fts_rank_cd_expr,
    fts_rank_cd_weight_array,
    fts_resolve_tsvector_expr,
    fts_tsquery_expr,
    fts_tsquery_expr_conjunction,
    fts_tsquery_expr_disjunction,
)

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #

_DEFAULT_JOIN: Final[tuple[tuple[str, str], ...]] = ((ID_FIELD, ID_FIELD),)

_FILTERED_CTE_ALIAS: Final[str] = "f"
_INDEX_ALIAS: Final[str] = "t"
_PROJECTION_ALIAS: Final[str] = "v"
_SCORED_CTE_ALIAS: Final[str] = "s"
_RANK_COLUMN: Final[str] = "_fts_rank"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFTSSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    SearchQueryPort[M],
    TxScopedPort,
):
    """FTS :class:`SearchQueryPort` using a projection relation and index heap.

    Structured filters (and tenant scope) apply on the **projection** relation
    (:attr:`~PostgresGateway.qname`), typically a view. Matching and
    ``ts_rank_cd`` use the **index heap** (:attr:`index_heap_qname`) and the
    ``tsvector`` expression from :attr:`index_qname`, mirroring
    :class:`PostgresPGroongaSearchAdapterV2`.

    Query shape (simplified)::

        WITH filtered AS (
            SELECT v.<join keys> FROM <projection> v WHERE <filters>
        ),
        scored AS (
            SELECT t.<heap cols> AS <proj keys>, <rank>
            FROM <index heap> t
            INNER JOIN filtered f ON <join>
            WHERE <fts match or TRUE>
        )
        SELECT v.<read fields>
        FROM <projection> v
        INNER JOIN scored s ON <join to projection keys>
        ORDER BY s.<rank> DESC NULLS LAST, <user sorts on v>
    """

    spec: SearchSpec[M]
    """Search specification."""

    index_qname: PostgresQualifiedName
    """Qualified name of the FTS index (resolves the ``tsvector`` expression)."""

    index_heap_qname: PostgresQualifiedName
    """Index heap qualified name (relation the index is built on)."""

    fts_groups: dict[FtsGroupLetter, Sequence[str]]
    """Mapping of FTS weight letters to field names."""

    join_pairs: Sequence[tuple[str, str]] | None = attrs.field(default=None)
    """Join pairs (projection column, index heap column)."""

    index_field_map: Mapping[str, str] | None = attrs.field(default=None)
    """Reserved for API symmetry with PGroonga v2; FTS uses the catalog ``tsvector``."""

    snapshot_coord: SearchResultSnapshotCoordinator | None = None
    """Coordinator for KV ordered-ID snapshots (same request surface as before)."""

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

    def _scored_key_columns(self) -> sql.Composable:
        return sql.SQL(", ").join(
            sql.SQL("{} AS {}").format(
                sql.Identifier(_INDEX_ALIAS, ic),
                sql.Identifier(pc),
            )
            for pc, ic in self._safe_join_pairs
        )

    # ....................... #

    def _scored_select_empty_query(
        self,
    ) -> tuple[sql.Composable, sql.Composable, list[Any]]:
        """Keys, zero rank, no extra rank parameters."""

        key_cols = self._scored_key_columns()
        rank = sql.SQL("(0)::double precision AS {}").format(
            sql.Identifier(_RANK_COLUMN),
        )
        return key_cols, rank, []

    # ....................... #

    def _scored_select_with_rank(
        self,
        *,
        tsv: sql.Composable,
        tsw_rank: sql.Composable,
        options: SearchOptions | None,
    ) -> tuple[sql.Composable, sql.Composable, list[Any]]:
        """Keys, ``ts_rank_cd`` column, parameters for rank (weights + tsquery)."""

        key_cols = self._scored_key_columns()
        gw = fts_effective_group_weights(self.spec, self.fts_groups, options)
        fts_weights = fts_rank_cd_weight_array(gw)
        rank_expr = sql.SQL("{} AS {}").format(
            fts_rank_cd_expr(tsv=tsv, tsw=tsw_rank),
            sql.Identifier(_RANK_COLUMN),
        )

        return key_cols, rank_expr, [fts_weights]

    # ....................... #

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: None = None,
        return_fields: None = None,
    ) -> CountlessPage[M]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: None = None,
        return_fields: None = None,
    ) -> Page[M]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> Page[JsonDict]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: type[T],
        return_fields: None = None,
    ) -> CountlessPage[T]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: type[T],
        return_fields: None = None,
    ) -> Page[T]: ...

    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        options = search_options_for_simple_adapter(options)

        rs_spec = self.spec.snapshot
        fp_fingerprint = SearchResultSnapshotCoordinator.simple_search_fingerprint(
            query,
            filters,
            sorts,
            spec_name=self.spec.name,
            variant="fts",
            extras={"phrase_combine": str(effective_phrase_combine(options))},
        )

        if self.snapshot_coord is not None and rs_spec is not None:
            maybe_snap: Any = await self.snapshot_coord.read_simple_result_snapshot(
                rs_spec=rs_spec,
                snap_opt=snapshot,
                fp_computed=fp_fingerprint,
                spec=self.spec,
                pagination=dict(pagination or {}),
                return_type=return_type,
                return_fields=return_fields,
                return_count=return_count,
            )
            if maybe_snap is not None:
                return maybe_snap

        fw, fp = await self.where_clause(filters)

        terms = normalize_search_queries(query)

        params_body: list[Any]
        if not terms:
            sw = sql.SQL("TRUE")  # type: ignore[assignment]
            scored_keys, scored_rank, _rank_extra = self._scored_select_empty_query()
            params_body = []

        else:
            tsv = await fts_resolve_tsvector_expr(self.introspector, self.index_qname)

            if len(terms) == 1:
                tsw_where, tsp_w = fts_tsquery_expr(terms[0], options=options)
                tsw_rank = tsw_where
                tsp_r = tsp_w
            else:
                fn = (
                    fts_tsquery_expr_disjunction
                    if effective_phrase_combine(options) == "any"
                    else fts_tsquery_expr_conjunction
                )
                tsw_where, tsp_w = fn(terms, options=options)
                tsw_rank, tsp_r = fn(terms, options=options)
            sw = fts_match_predicate(tsv=tsv, tsw=tsw_where)  # type: ignore[assignment]
            scored_keys, scored_rank, rank_w = self._scored_select_with_rank(
                tsv=tsv,
                tsw_rank=tsw_rank,
                options=options,
            )
            # ``WITH filtered`` uses ``fp``; ``scored`` SELECT lists ``ts_rank_cd`` before
            # ``WHERE``, so: ``fp``, ``weights``, rank ``tsquery``, where ``tsquery``.
            # Single term: one ``fts_tsquery_expr``; ``tsw_rank``/``tsp`` match ``tsw_where``;
            # params still list the tsquery twice (two ``Placeholder``\ s in the SQL).
            params_body = [*fp, *rank_w, *tsp_r, *tsp_w]

        key_sel = self._filtered_select_list()

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

        order_parts: list[sql.Composable] = [
            sql.SQL("{} DESC NULLS LAST").format(
                sql.Identifier(_SCORED_CTE_ALIAS, _RANK_COLUMN),
            )
        ]
        extra_ob = await self._projection_order_by_clause(sorts)

        if extra_ob is not None:
            order_parts.append(extra_ob)

        order_sql = sql.SQL(", ").join(order_parts)
        with_clause = sql.SQL("WITH {}{}").format(filtered_cte, scored_cte)

        params_count = [*fp] if not terms else params_body

        count_stmt = sql.SQL(
            """
            {with_clause}
            SELECT COUNT(*) {from_outer}
            """
        ).format(with_clause=with_clause, from_outer=from_outer)

        total = 0

        if return_count:
            total = int(
                await self.client.fetch_value(count_stmt, params_count, default=0),
            )
            if total == 0:
                return page_from_limit_offset(  # pyright: ignore[reportUnknownVariableType]
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

        params = [*fp] if not terms else params_body

        pagination = pagination or {}
        want_snap = (
            self.snapshot_coord is not None
            and rs_spec is not None
            and self.snapshot_coord.should_write_result_snapshot(snapshot, rs_spec)
        )

        max_nw = (
            self.snapshot_coord.effective_snapshot_max_ids(snapshot, rs_spec)
            if want_snap and self.snapshot_coord is not None
            else 0
        )
        sql_limit, sql_offset, page_limit = (
            SearchResultSnapshotCoordinator.snapshot_pagination(
                want_snap, max_nw, dict(pagination)
            )
        )
        if sql_limit is not None:
            data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(int(sql_limit))

        if want_snap:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(sql_offset))

        elif pagination.get("offset") is not None:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(pagination.get("offset") or 0))

        rows = await self.client.fetch_all(data_stmt, params, row_factory="dict")

        handle_out = None
        if want_snap and self.snapshot_coord is not None and rs_spec is not None:
            pool_len = len(rows)
            pool = pydantic_validate_many(self.model_type, rows)
            handle_out = await self.snapshot_coord.put_simple_ordered_hits(
                pool,
                snap_opt=snapshot,
                rs_spec=rs_spec,
                fp_computed=fp_fingerprint,
                pool_len_before_cap=pool_len,
            )
            u_off = int(pagination.get("offset") or 0)
            rows = rows[u_off : u_off + page_limit]

        if return_type is not None:
            v = pydantic_validate_many(return_type, rows)
            if return_count:
                return page_from_limit_offset(
                    v,
                    pagination,
                    total=total,
                    snapshot=handle_out,
                )
            return page_from_limit_offset(
                v,
                pagination,
                total=None,
                snapshot=handle_out,
            )

        if return_fields is not None:
            raw = [{k: r.get(k, None) for k in return_fields} for r in rows]
            if return_count:
                return page_from_limit_offset(
                    raw,
                    pagination,
                    total=total,
                    snapshot=handle_out,
                )
            return page_from_limit_offset(
                raw,
                pagination,
                total=None,
                snapshot=handle_out,
            )

        m = pydantic_validate_many(self.model_type, rows)
        if return_count:
            return page_from_limit_offset(
                m,
                pagination,
                total=total,
                snapshot=handle_out,
            )
        return page_from_limit_offset(
            m,
            pagination,
            total=None,
            snapshot=handle_out,
        )

    # ....................... #

    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> CountlessPage[M]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_type=None,
            return_fields=None,
        )

    async def search_page(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Page[M]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_type=None,
            return_fields=None,
        )

    async def project_search(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_type=None,
            return_fields=tuple(fields),
        )

    async def project_search_page(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Page[JsonDict]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_type=None,
            return_fields=tuple(fields),
        )

    async def select_search(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> CountlessPage[T]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_type=return_type,
            return_fields=None,
        )

    async def select_search_page(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Page[T]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    @overload
    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: None = None,
        return_fields: None = None,
    ) -> CursorPage[M]: ...

    @overload
    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> CursorPage[JsonDict]: ...

    @overload
    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T],
        return_fields: None = None,
    ) -> CursorPage[T]: ...

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
        """Keyset on the projection (empty query) or ranked ``ts_rank_cd`` matches."""

        options = search_options_for_simple_adapter(options)
        terms = normalize_search_queries(query)

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

        if not terms:
            if sorts is None:
                first = sorted(self.read_fields)[0]
                key_spec: list[tuple[str, str]] = [(first, "asc"), (ID_FIELD, "asc")]

            else:
                key_spec = list(normalize_sorts_with_id(sorts))

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

                sk, sp_seek = build_seek_condition(
                    exprs,
                    directions,
                    list(tv),
                    "before" if use_before else "after",
                )

                where_fin = sql.SQL("({} AND ({}))").format(fw, sk)
                params = params + sp_seek

            order_sql = build_order_by_sql(exprs, directions, flip=use_before)
            cols = self.return_clause(
                return_type,
                select_rf,
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

            def _row_token_vals_browse(row: JsonDict) -> list[Any]:
                return [row_value_for_sort_key(row, k) for k in sort_keys]

            if has_more and rows:
                nxt = encode_keyset_v1(
                    sort_keys=sort_keys,
                    directions=directions,
                    values=_row_token_vals_browse(rows[-1]),
                )

            else:
                nxt = None

            if rows and (use_after or (use_before and has_more)):
                prv = encode_keyset_v1(
                    sort_keys=sort_keys,
                    directions=directions,
                    values=_row_token_vals_browse(rows[0]),
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

        fw_r, fp_r = await self.where_clause(filters)
        tsv = await fts_resolve_tsvector_expr(self.introspector, self.index_qname)

        if len(terms) == 1:
            tsw_where, tsp_w = fts_tsquery_expr(terms[0], options=options)
            tsw_rank = tsw_where
            tsp_r = tsp_w

        else:
            fn = (
                fts_tsquery_expr_disjunction
                if effective_phrase_combine(options) == "any"
                else fts_tsquery_expr_conjunction
            )
            tsw_where, tsp_w = fn(terms, options=options)
            tsw_rank, tsp_r = fn(terms, options=options)

        sw_r = fts_match_predicate(tsv=tsv, tsw=tsw_where)
        scored_keys_r, scored_rank_r, rank_w = self._scored_select_with_rank(
            tsv=tsv,
            tsw_rank=tsw_rank,
            options=options,
        )
        params_base_r = [*fp_r, *rank_w, *tsp_r, *tsp_w]

        key_spec_r = ranked_search_cursor_key_spec(
            rank_field=_RANK_COLUMN,
            sorts=sorts,
        )
        sort_keys_r = [k for k, _ in key_spec_r]
        directions_r = [d for _, d in key_spec_r]

        filtered_cte = sql.SQL(
            """
            {filtered} AS (
                SELECT {key_sel}
                FROM {proj} {pa}
                WHERE {fw}
            )"""
        ).format(
            filtered=sql.Identifier(_FILTERED_CTE_ALIAS),
            key_sel=self._filtered_select_list(),
            proj=self.source_qname.ident(),
            pa=sql.Identifier(_PROJECTION_ALIAS),
            fw=fw_r,
        )

        join_sf_r = self._scored_join_on_filtered()
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
            scored_keys=scored_keys_r,
            scored_rank=scored_rank_r,
            heap=self.index_heap_qname.ident(),
            ia=sql.Identifier(_INDEX_ALIAS),
            filtered=sql.Identifier(_FILTERED_CTE_ALIAS),
            fa=sql.Identifier(_FILTERED_CTE_ALIAS),
            join_sf=join_sf_r,
            sw=sw_r,
        )

        join_vs_r = self._outer_join_on_scored()
        from_outer_r = sql.SQL(
            """
            FROM {proj} {pa}
            INNER JOIN {scored} {sa} ON ({join_vs})
            """
        ).format(
            proj=self.source_qname.ident(),
            pa=sql.Identifier(_PROJECTION_ALIAS),
            scored=sql.Identifier(_SCORED_CTE_ALIAS),
            sa=sql.Identifier(_SCORED_CTE_ALIAS),
            join_vs=join_vs_r,
        )

        with_clause_r = sql.SQL("WITH {}{}").format(filtered_cte, scored_cte)

        types_r = await self.column_types()
        exprs_r: list[sql.Composable] = []
        for k in sort_keys_r:
            if k == _RANK_COLUMN:
                exprs_r.append(sql.Identifier(_SCORED_CTE_ALIAS, _RANK_COLUMN))
            else:
                exprs_r.append(
                    sort_key_expr(
                        field=k,
                        column_types=types_r,
                        model_type=self.model_type,
                        nested_field_hints=self.nested_field_hints,
                        table_alias=_PROJECTION_ALIAS,
                    )
                )

        where_fin_r: sql.Composable = sql.SQL("TRUE")
        params_r: list[Any] = list(params_base_r)

        if use_after or use_before:
            token_r = str(c["after" if use_after else "before"])
            tk_r, td_r, tv_r = decode_keyset_v1(token_r)

            if tk_r != sort_keys_r or len(td_r) != len(directions_r):
                raise CoreError("Cursor does not match current search sort")

            for i, di in enumerate(directions_r):
                if (td_r[i] or "").lower() != di:
                    raise CoreError("Cursor does not match current search sort")

            sk_r, sp_seek_r = build_seek_condition(
                exprs_r,
                directions_r,
                list(tv_r),
                "before" if use_before else "after",
            )

            where_fin_r = sk_r
            params_r = params_r + sp_seek_r

        order_sql_r = build_ranked_cursor_order_by_sql(
            exprs_r,
            sort_keys_r,
            directions_r,
            rank_key=_RANK_COLUMN,
            flip=use_before,
        )

        return_fields_sql_r: Sequence[str] | None
        if return_fields is not None:
            return_fields_sql_r = cursor_return_fields_for_select(
                sort_keys=sort_keys_r,
                rank_field=_RANK_COLUMN,
                return_fields=return_fields,
            )
            if not return_fields_sql_r:
                return_fields_sql_r = None
        else:
            return_fields_sql_r = None

        base_cols_r = self.return_clause(
            return_type,
            return_fields_sql_r,
            table_alias=_PROJECTION_ALIAS,
        )
        cols_r = sql.SQL("{}, {}").format(
            base_cols_r,
            sql.SQL("{} AS {}").format(
                sql.Identifier(_SCORED_CTE_ALIAS, _RANK_COLUMN),
                sql.Identifier(_RANK_COLUMN),
            ),
        )

        data_stmt_r = sql.SQL(
            """
            {with_clause}
            SELECT {cols} {from_outer}
            WHERE {w}
            ORDER BY {order}
            """
        ).format(
            with_clause=with_clause_r,
            cols=cols_r,
            from_outer=from_outer_r,
            w=where_fin_r,
            order=order_sql_r,
        )
        data_stmt_r = sql.SQL("{} LIMIT {}").format(
            data_stmt_r,
            sql.Placeholder(),
        )
        params_r.append(lim + 1)

        raw_rows_r = list(
            await self.client.fetch_all(data_stmt_r, params_r, row_factory="dict")
        )  # type: ignore[assignment, arg-type]

        if use_before:
            raw_rows_r = list(reversed(raw_rows_r))

        has_more_r = len(raw_rows_r) > lim
        rows_r = raw_rows_r[:lim]

        def _row_token_vals_ranked(row: JsonDict) -> list[Any]:
            return [row_value_for_sort_key(row, k) for k in sort_keys_r]

        if has_more_r and rows_r:
            nxt_r = encode_keyset_v1(
                sort_keys=sort_keys_r,
                directions=directions_r,
                values=_row_token_vals_ranked(rows_r[-1]),
            )

        else:
            nxt_r = None

        if rows_r and (use_after or (use_before and has_more_r)):
            prv_r = encode_keyset_v1(
                sort_keys=sort_keys_r,
                directions=directions_r,
                values=_row_token_vals_ranked(rows_r[0]),
            )

        else:
            prv_r = None

        if return_type is not None:
            v_r = pydantic_validate_many(return_type, rows_r)

            return CursorPage(
                hits=v_r,
                next_cursor=nxt_r,
                prev_cursor=prv_r,
                has_more=has_more_r,
            )
        if return_fields is not None:
            rj_r = [{k: r.get(k, None) for k in return_fields} for r in rows_r]

            return CursorPage(
                hits=rj_r,
                next_cursor=nxt_r,
                prev_cursor=prv_r,
                has_more=has_more_r,
            )

        m_r = pydantic_validate_many(self.model_type, rows_r)

        return CursorPage(
            hits=m_r,
            next_cursor=nxt_r,
            prev_cursor=prv_r,
            has_more=has_more_r,
        )

    async def search_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[M]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=None,
            return_fields=None,
        )

    async def project_search_cursor(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[JsonDict]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=None,
            return_fields=tuple(fields),
        )

    async def select_search_cursor(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[T]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=return_type,
            return_fields=None,
        )
