"""Base class for projection + index-heap Postgres search adapters."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    normalize_search_queries,
    search_options_for_simple_adapter,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import exc
from forze_postgres.kernel.relation import RelationSpec, is_static_relation, resolve_postgres_qname

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ._cursor_run import (
    execute_projection_keyset_cursor,
    execute_ranked_pipeline_cursor,
    parse_search_cursor,
)
from ._engine import RankedPipelineSql
from ._offset_run import RankedOffsetPlan, execute_simple_ranked_offset_search
from ._pipeline_sql import PipelineAliases, build_rank_first_order
from ._port import PostgresSearchPortMixin

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresRankedPipelineSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    PostgresSearchPortMixin[M],
    SearchQueryPort[M],
):
    """Shared offset/cursor execution for FTS, vector, and PGroonga search adapters."""

    index_relation: RelationSpec
    """FTS/PGroonga index or vector index relation."""

    index_heap_relation: RelationSpec
    """Heap relation the index is defined on."""

    _index_qname_resolved: PostgresQualifiedName | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )
    _index_heap_qname_resolved: PostgresQualifiedName | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )

    search_variant: str = attrs.field()
    """Snapshot fingerprint variant (e.g. ``fts``, ``vector``, ``pgroonga``)."""

    pipeline: PipelineAliases = attrs.field()
    """CTE aliases for the filtered → scored → projection pipeline."""

    search_rank_column: str = attrs.field()
    """Rank column inside the scored CTE."""

    projection_alias: str = "v"
    """SQL alias for the read projection in outer queries."""

    result_snapshot: SearchResultSnapshot | None = attrs.field(default=None)
    """Optional result-ID snapshot coordinator."""

    # ....................... #

    async def _index_qname(self) -> PostgresQualifiedName:
        if self._index_qname_resolved is not None:
            return self._index_qname_resolved

        resolved = await resolve_postgres_qname(
            self.index_relation,
            self._tenant_id_for_resolve(),
        )
        object.__setattr__(self, "_index_qname_resolved", resolved)

        return resolved

    # ....................... #

    async def _index_heap_qname(self) -> PostgresQualifiedName:
        if self._index_heap_qname_resolved is not None:
            return self._index_heap_qname_resolved

        resolved = await resolve_postgres_qname(
            self.index_heap_relation,
            self._tenant_id_for_resolve(),
        )
        object.__setattr__(self, "_index_heap_qname_resolved", resolved)

        return resolved

    # ....................... #

    @property
    def index_qname(self) -> PostgresQualifiedName:
        """Best-effort sync access when :attr:`index_relation` is static."""

        if self._index_qname_resolved is not None:
            return self._index_qname_resolved

        if is_static_relation(self.index_relation):
            return PostgresQualifiedName(*self.index_relation)

        raise exc.internal(
            "index_qname is only available for static index_relation; use await _index_qname()",
        )

    # ....................... #

    @property
    def index_heap_qname(self) -> PostgresQualifiedName:
        """Best-effort sync access when :attr:`index_heap_relation` is static."""

        if self._index_heap_qname_resolved is not None:
            return self._index_heap_qname_resolved

        if is_static_relation(self.index_heap_relation):
            return PostgresQualifiedName(*self.index_heap_relation)

        raise exc.internal(
            "index_heap_qname is only available for static index_heap_relation; "
            "use await _index_heap_qname()",
        )

    # ....................... #

    async def _build_ranked_pipeline_sql(
        self,
        *,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        options: SearchOptions | None,
        fw: sql.Composable,
        fp: list[Any],
        terms: tuple[str, ...],
        pagination: PaginationExpression | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        parsed_filters: Any = None,
    ) -> RankedPipelineSql:
        """Assemble pipeline CTEs; engine-specific leg SQL is built inside subclasses."""

        raise NotImplementedError

    # ....................... #

    def _fingerprint_extras(
        self,
        options: SearchOptions | None,
        **kwargs: object,
    ) -> dict[str, object] | None:
        _ = options, kwargs
        return None

    # ....................... #

    async def _projection_order_by_clause(
        self,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable | None:
        return await self.order_by_clause(sorts, table_alias=self.projection_alias)

    # ....................... #

    async def _offset_search_impl(  # type: ignore[override]
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool = False,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        options = search_options_for_simple_adapter(options)
        parsed_filters = self.compile_filters(filters)
        fw, fp = await self.where_clause(filters, parsed=parsed_filters)
        terms = tuple(normalize_search_queries(query))
        pipeline_sql = await self._build_ranked_pipeline_sql(
            query=query,
            filters=filters,
            options=options,
            fw=fw,
            fp=fp,
            terms=terms,
            pagination=pagination,
            snapshot=snapshot,
            parsed_filters=parsed_filters,
        )
        extra_ob = await self._projection_order_by_clause(sorts)
        order_sql = build_rank_first_order(
            aliases=self.pipeline,
            extra_order=extra_ob,
        )

        plan = RankedOffsetPlan(
            with_clause=pipeline_sql.with_clause,
            from_outer=pipeline_sql.from_outer,
            order_sql=order_sql,
            params=pipeline_sql.params_body,
            count_params=pipeline_sql.count_params,
            select_table_alias=self.projection_alias,
        )

        fp_extras = self._fingerprint_extras(
            options,
            resolved_plan=getattr(pipeline_sql, "resolved_plan", None),
            candidate_limit=getattr(pipeline_sql, "candidate_limit", None),
        )

        return await execute_simple_ranked_offset_search(
            self,
            plan=plan,
            query=query,
            filters=filters,
            sorts=sorts,
            spec=self.spec,
            variant=self.search_variant,
            fingerprint_extras=fp_extras,
            pagination=pagination,
            snapshot=snapshot,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
            model_type=self.model_type,
            result_snapshot=self.result_snapshot,
        )

    # ....................... #

    async def _cursor_search_impl(  # type: ignore[override]
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        options = search_options_for_simple_adapter(options)
        lim, _, _ = parse_search_cursor(cursor)
        terms = tuple(normalize_search_queries(query))
        parsed_filters = self.compile_filters(filters)

        if not terms:
            return await execute_projection_keyset_cursor(
                self,
                filters=filters,
                cursor=cursor,
                sorts=sorts,
                spec=self.spec,
                projection_alias=self.projection_alias,
                parsed_filters=parsed_filters,
                return_type=return_type,
                return_fields=return_fields,
            )

        fw, fp = await self.where_clause(filters, parsed=parsed_filters)
        pipeline_sql = await self._build_ranked_pipeline_sql(
            query=query,
            filters=filters,
            options=options,
            fw=fw,
            fp=fp,
            terms=terms,
            pagination={"limit": lim},
            snapshot=None,
            parsed_filters=parsed_filters,
        )

        return await execute_ranked_pipeline_cursor(
            self,
            pipeline_sql=pipeline_sql,
            filters=filters,
            cursor=cursor,
            sorts=sorts,
            spec=self.spec,
            return_type=return_type,
            return_fields=return_fields,
        )
