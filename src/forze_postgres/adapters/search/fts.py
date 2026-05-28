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
)
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    decode_keyset_v1,
    encode_keyset_v1,
    normalize_sorts_for_keyset,
    resolve_effective_sorts,
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
from forze.application.coordinators import SearchResultSnapshotCoordinator
from forze.base.exceptions import exc
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
from ._fts_sql import FtsGroupLetter
from ._leg_fts import build_fts_leg
from ._offset_run import RankedOffsetPlan, execute_simple_ranked_offset_search
from ._pipeline_sql import (
    PipelineAliases,
    build_filtered_cte,
    build_outer_from,
    build_pipeline_with_clause,
    build_rank_first_order,
    build_scored_cte,
    filtered_select_list,
    outer_join_on_scored,
    scored_join_on_filtered,
    scored_key_columns,
    validate_join_pairs,
)

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #

_DEFAULT_JOIN: Final[tuple[tuple[str, str], ...]] = ((ID_FIELD, ID_FIELD),)

_PROJECTION_ALIAS: Final[str] = "v"
_RANK_COLUMN: Final[str] = "_fts_rank"
_PIPELINE: Final[PipelineAliases] = PipelineAliases(rank_column=_RANK_COLUMN)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFTSSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    SearchQueryPort[M],
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

    # ....................... #

    @property
    def _safe_join_pairs(self) -> Sequence[tuple[str, str]]:
        return self.join_pairs or _DEFAULT_JOIN

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        validate_join_pairs(self._safe_join_pairs)

    # ....................... #

    async def _projection_order_by_clause(
        self,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable | None:
        return await self.order_by_clause(sorts, table_alias=_PROJECTION_ALIAS)

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
        fw, fp = await self.where_clause(filters)
        terms = tuple(normalize_search_queries(query))
        join = self._safe_join_pairs

        sw, scored_rank, leg_params = await build_fts_leg(
            introspector=self.introspector,
            index_qname=self.index_qname,
            search=self.spec,
            fts_groups=self.fts_groups,
            index_alias=_PIPELINE.index,
            queries=terms,
            options=options,
            score_column=_RANK_COLUMN,
        )
        scored_keys = scored_key_columns(join, index_alias=_PIPELINE.index)
        params_body = [*fp, *leg_params]

        key_sel = filtered_select_list(join, projection_alias=_PIPELINE.projection)
        filtered_cte = build_filtered_cte(
            aliases=_PIPELINE,
            key_sel=key_sel,
            proj_ident=self.source_qname.ident(),
            fw=fw,
        )
        join_sf = scored_join_on_filtered(
            join,
            index_alias=_PIPELINE.index,
            filtered_alias=_PIPELINE.filtered,
        )
        scored_cte = build_scored_cte(
            aliases=_PIPELINE,
            scored_keys=scored_keys,
            scored_rank=scored_rank,
            heap_ident=self.index_heap_qname.ident(),
            join_sf=join_sf,
            sw=sw,
        )
        join_vs = outer_join_on_scored(
            join,
            projection_alias=_PIPELINE.projection,
            scored_alias=_PIPELINE.scored,
        )
        from_outer = build_outer_from(
            aliases=_PIPELINE,
            proj_ident=self.source_qname.ident(),
            join_vs=join_vs,
        )
        extra_ob = await self._projection_order_by_clause(sorts)
        order_sql = build_rank_first_order(aliases=_PIPELINE, extra_order=extra_ob)
        with_clause = build_pipeline_with_clause(filtered_cte, scored_cte)

        plan = RankedOffsetPlan(
            with_clause=with_clause,
            from_outer=from_outer,
            order_sql=order_sql,
            params=params_body,
            count_params=[*fp] if not terms else None,
            select_table_alias=_PROJECTION_ALIAS,
        )

        return await execute_simple_ranked_offset_search(
            self,
            plan=plan,
            query=query,
            filters=filters,
            sorts=sorts,
            spec=self.spec,
            variant="fts",
            fingerprint_extras={
                "phrase_combine": str(effective_phrase_combine(options)),
            },
            pagination=pagination,
            snapshot=snapshot,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
            model_type=self.model_type,
            snapshot_coord=self.snapshot_coord,
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

    # ....................... #

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

    # ....................... #

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

    # ....................... #

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

    # ....................... #

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

    # ....................... #

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
            raise exc.internal(
                "Cursor pagination: pass at most one of 'after' or 'before'"
            )

        lim: int = 10 if c.get("limit") is None else int(c["limit"])  # type: ignore[arg-type, assignment, call-overload]

        if lim < 1:
            raise exc.internal("Cursor pagination 'limit' must be positive")

        use_after = c.get("after") is not None
        use_before = c.get("before") is not None

        parsed_filters = self.compile_filters(filters)

        if not terms:
            effective = resolve_effective_sorts(
                sorts=sorts,
                default_sort=self.spec.default_sort,
                read_fields=self.read_fields,
                spec_name=self.spec.name,
            )
            key_spec = list(
                normalize_sorts_for_keyset(
                    effective,
                    read_fields=self.read_fields,
                )
            )

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

            fw, fp = await self.where_clause(filters, parsed=parsed_filters)
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
                    raise exc.internal("Cursor does not match current search sort")

                for i, di in enumerate(directions):
                    if (td[i] or "").lower() != di:
                        raise exc.internal("Cursor does not match current search sort")

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

        fw_r, fp_r = await self.where_clause(filters, parsed=parsed_filters)
        join_r = self._safe_join_pairs
        term_tuple = tuple(terms)

        sw_r, scored_rank_r, leg_params_r = await build_fts_leg(
            introspector=self.introspector,
            index_qname=self.index_qname,
            search=self.spec,
            fts_groups=self.fts_groups,
            index_alias=_PIPELINE.index,
            queries=term_tuple,
            options=options,
            score_column=_RANK_COLUMN,
        )
        scored_keys_r = scored_key_columns(join_r, index_alias=_PIPELINE.index)
        params_base_r = [*fp_r, *leg_params_r]

        user_sorts = sorts if sorts else self.spec.default_sort

        key_spec_r = ranked_search_cursor_key_spec(
            rank_field=_RANK_COLUMN,
            sorts=user_sorts,
            read_fields=self.read_fields,
        )
        sort_keys_r = [k for k, _ in key_spec_r]
        directions_r = [d for _, d in key_spec_r]

        key_sel_r = filtered_select_list(join_r, projection_alias=_PIPELINE.projection)
        filtered_cte_r = build_filtered_cte(
            aliases=_PIPELINE,
            key_sel=key_sel_r,
            proj_ident=self.source_qname.ident(),
            fw=fw_r,
        )
        join_sf_r = scored_join_on_filtered(
            join_r,
            index_alias=_PIPELINE.index,
            filtered_alias=_PIPELINE.filtered,
        )
        scored_cte_r = build_scored_cte(
            aliases=_PIPELINE,
            scored_keys=scored_keys_r,
            scored_rank=scored_rank_r,
            heap_ident=self.index_heap_qname.ident(),
            join_sf=join_sf_r,
            sw=sw_r,
        )
        join_vs_r = outer_join_on_scored(
            join_r,
            projection_alias=_PIPELINE.projection,
            scored_alias=_PIPELINE.scored,
        )
        from_outer_r = build_outer_from(
            aliases=_PIPELINE,
            proj_ident=self.source_qname.ident(),
            join_vs=join_vs_r,
        )

        with_clause_r = build_pipeline_with_clause(filtered_cte_r, scored_cte_r)

        types_r = await self.column_types()
        exprs_r: list[sql.Composable] = []

        for k in sort_keys_r:
            if k == _RANK_COLUMN:
                exprs_r.append(sql.Identifier(_PIPELINE.scored, _RANK_COLUMN))

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
                raise exc.internal("Cursor does not match current search sort")

            for i, di in enumerate(directions_r):
                if (td_r[i] or "").lower() != di:
                    raise exc.internal("Cursor does not match current search sort")

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
                sql.Identifier(_PIPELINE.scored, _RANK_COLUMN),
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

    # ....................... #

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

    # ....................... #

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

    # ....................... #

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
