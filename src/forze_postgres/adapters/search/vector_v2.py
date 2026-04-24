"""Vector (pgvector) search with projection vs index-heap separation (CTE pipeline)."""

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
from forze.application.contracts.embeddings import (
    EmbeddingsProviderPort,
    EmbeddingsSpec,
)
from forze.application.contracts.query import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    SearchResultSnapshotPort,
    SearchSpec,
    effective_phrase_combine,
    normalize_search_queries,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many
from forze.domain.constants import ID_FIELD

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ..txmanager import PostgresTxScopeKey
from ._options import search_options_for_simple_adapter
from ._vector_sql import (
    VectorDistanceKind,
    assert_embedding_shape,
    vector_knn_multi_score_expr,
    vector_knn_score_expr,
    vector_param_literal,
)
from .federated_snapshot import effective_snapshot_max_ids
from .result_snapshot_ops import (
    put_simple_result_snapshot,
    read_simple_result_snapshot,
    should_write_result_snapshot,
    simple_search_fingerprint,
    snapshot_sql_pagination,
)

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

_DEFAULT_JOIN: Final[tuple[tuple[str, str], ...]] = ((ID_FIELD, ID_FIELD),)

_FILTERED_CTE_ALIAS: Final[str] = "f"
_INDEX_ALIAS: Final[str] = "t"
_PROJECTION_ALIAS: Final[str] = "v"
_SCORED_CTE_ALIAS: Final[str] = "s"
_RANK_COLUMN: Final[str] = "_vector_rank"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresVectorSearchAdapterV2[M: BaseModel](
    PostgresGateway[M],
    SearchQueryPort[M],
    TxScopedPort,
):
    """pgvector :class:`SearchQueryPort`: KNN on a heap column with projection filters."""

    spec: SearchSpec[M]
    """Search specification."""

    index_qname: PostgresQualifiedName
    """Qualified name for configuration symmetry (index object); not read at query time."""

    index_heap_qname: PostgresQualifiedName
    """Heap that holds the ``vector`` column used for distance scoring."""

    embedder: EmbeddingsProviderPort
    """Text-to-vector (query string encoding)."""

    embeddings_spec: EmbeddingsSpec
    """Expected vector dimension; must match the ``vector`` column and embedder output."""

    vector_column: str
    """Heap column with type ``vector`` (or compatible)."""

    vector_distance: VectorDistanceKind = "l2"
    """pgvector distance operator family (``<->`` / ``<=>`` / ``<#>``)."""

    join_pairs: Sequence[tuple[str, str]] | None = attrs.field(default=None)
    """Join pairs (projection column, index heap column)."""

    index_field_map: Mapping[str, str] | None = attrs.field(default=None)
    """Optional map from :class:`SearchSpec` field names to heap column names (unused in v2)."""

    snapshot_store: SearchResultSnapshotPort | None = None
    """Optional store for ordered row keys to accelerate paged reads."""

    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)
    """Transaction scope."""

    # ....................... #

    @property
    def _safe_join_pairs(self) -> Sequence[tuple[str, str]]:
        return self.join_pairs or _DEFAULT_JOIN

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _ = self.index_qname, self.index_field_map
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
        snap_raw = (options or {}).get("result_snapshot")
        snap_opt: SearchResultSnapshotOptions | None = (
            snap_raw if isinstance(snap_raw, dict) else None
        )
        rs_spec = self.spec.result_snapshot
        fp_fingerprint = simple_search_fingerprint(
            query,
            filters,
            sorts,
            spec_name=self.spec.name,
            variant="vector",
            extras={
                "phrase_combine": str(effective_phrase_combine(options)),
                "embeddings": str(self.embeddings_spec.name),
                "vector_column": str(self.vector_column),
                "vector_distance": str(self.vector_distance),
                "embeddings_dim": int(self.embeddings_spec.dimensions),
            },
        )
        if self.snapshot_store is not None and rs_spec is not None:
            maybe_snap: Any = await read_simple_result_snapshot(
                store=self.snapshot_store,
                rs_spec=rs_spec,
                snap_opt=snap_opt,
                fp_computed=fp_fingerprint,
                spec=self.spec,
                pagination=dict(pagination or {}),
                return_type=return_type,
                return_fields=return_fields,
                return_count=return_count,
            )
            if maybe_snap is not None:
                return maybe_snap

        combine = effective_phrase_combine(options)
        fw, fp = await self.where_clause(filters)

        key_cols = self._scored_key_columns()
        scored_rank: sql.Composable
        terms = normalize_search_queries(query)
        if not terms:
            sw = sql.SQL("TRUE")
            scored_rank = sql.SQL("(0)::double precision AS {}").format(
                sql.Identifier(_RANK_COLUMN),
            )
            params_body: list[Any] = [*fp]
        elif len(terms) == 1:
            one = await self.embedder.embed_one(terms[0], input_kind="query")
            assert_embedding_shape(
                one,
                expect_dim=self.embeddings_spec.dimensions,
            )
            sw = sql.SQL("TRUE")
            scored_rank = vector_knn_score_expr(
                index_alias=_INDEX_ALIAS,
                column=self.vector_column,
                kind=self.vector_distance,
                score_name=_RANK_COLUMN,
            )
            params_body = [*fp, vector_param_literal(one)]
        else:
            vecs = await self.embedder.embed(terms, input_kind="query")
            for vec in vecs:
                assert_embedding_shape(
                    vec,
                    expect_dim=self.embeddings_spec.dimensions,
                )
            sw = sql.SQL("TRUE")
            scored_rank = vector_knn_multi_score_expr(
                index_alias=_INDEX_ALIAS,
                column=self.vector_column,
                kind=self.vector_distance,
                score_name=_RANK_COLUMN,
                n_queries=len(vecs),
                phrase_combine=combine,
            )
            params_body = [*fp, *[vector_param_literal(v) for v in vecs]]

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
            scored_keys=key_cols,
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

        count_stmt = sql.SQL(
            """
            {with_clause}
            SELECT COUNT(*) {from_outer}
            """
        ).format(with_clause=with_clause, from_outer=from_outer)

        total = 0
        if return_count:
            total = int(
                await self.client.fetch_value(count_stmt, params_body, default=0),
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

        params = list(params_body)

        pagination = pagination or {}
        want_sn = (
            self.snapshot_store is not None
            and rs_spec is not None
            and should_write_result_snapshot(snap_opt, rs_spec)
        )
        max_nv = effective_snapshot_max_ids(snap_opt, rs_spec) if want_sn else 0
        sql_limit, sql_offset, page_limit = snapshot_sql_pagination(
            want_sn, max_nv, dict(pagination)
        )
        if sql_limit is not None:
            data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(int(sql_limit))
        if want_sn:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(sql_offset))
        elif pagination.get("offset") is not None:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(pagination.get("offset") or 0))

        rows = await self.client.fetch_all(data_stmt, params, row_factory="dict")

        handle_out = None
        if want_sn and self.snapshot_store is not None and rs_spec is not None:
            pl = len(rows)
            pool = pydantic_validate_many(self.model_type, rows)
            handle_out = await put_simple_result_snapshot(
                self.snapshot_store,
                pool,
                snap_opt=snap_opt,
                rs_spec=rs_spec,
                fp_computed=fp_fingerprint,
                pool_len_before_cap=pl,
            )
            uo = int(pagination.get("offset") or 0)
            rows = rows[uo : uo + page_limit]

        if return_type is not None:
            v = pydantic_validate_many(return_type, rows)
            if return_count:
                return page_from_limit_offset(
                    v, pagination, total=total, result_snapshot=handle_out
                )
            return page_from_limit_offset(
                v, pagination, total=None, result_snapshot=handle_out
            )

        if return_fields is not None:
            raw = [{k: r.get(k, None) for k in return_fields} for r in rows]
            if return_count:
                return page_from_limit_offset(
                    raw, pagination, total=total, result_snapshot=handle_out
                )
            return page_from_limit_offset(
                raw, pagination, total=None, result_snapshot=handle_out
            )

        m = pydantic_validate_many(self.model_type, rows)
        if return_count:
            return page_from_limit_offset(
                m, pagination, total=total, result_snapshot=handle_out
            )
        return page_from_limit_offset(
            m, pagination, total=None, result_snapshot=handle_out
        )

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
        del query, filters, cursor, sorts, options, return_type, return_fields
        raise CoreError(
            "search_with_cursor is not implemented for PostgresVectorSearchAdapterV2; use "
            "search() with limit/offset.",
        )
