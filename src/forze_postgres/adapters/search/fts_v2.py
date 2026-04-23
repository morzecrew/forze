"""FTS search with projection vs index-heap separation (CTE pipeline)."""

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

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ..txmanager import PostgresTxScopeKey
from ._options import search_options_for_simple_adapter
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

_DEFAULT_JOIN: Final[tuple[tuple[str, str], ...]] = ((ID_FIELD, ID_FIELD),)

_FILTERED_CTE_ALIAS: Final[str] = "f"
_INDEX_ALIAS: Final[str] = "t"
_PROJECTION_ALIAS: Final[str] = "v"
_SCORED_CTE_ALIAS: Final[str] = "s"
_RANK_COLUMN: Final[str] = "_fts_rank"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFTSSearchAdapterV2[M: BaseModel](
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

        params = [*fp] if not terms else params_body

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
        del query, filters, cursor, sorts, options, return_type, return_fields
        raise NotImplementedError(
            "PostgresFTSSearchAdapterV2.search_with_cursor is not implemented yet; "
            "see forze.application.contracts.search.ports module doc.",
        )
