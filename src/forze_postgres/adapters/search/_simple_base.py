"""Base class for projection + index-heap Postgres search adapters."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Sequence
from typing import Any, Literal

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryExpr,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    SearchCapabilities,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    normalize_search_queries,
    refuse_cursor_null_placement,
    resolve_search_sorts,
    search_options_for_simple_adapter,
)
from forze.application.integrations.search import (
    SearchResultSnapshot,
    reject_encrypted_sort_fields,
)
from forze.base.exceptions import exc
from forze.base.primitives import OnceCell
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.relation import (
    RelationSpec,
    is_static_relation,
    resolve_postgres_qname,
)

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ._cursor_run import (
    execute_projection_keyset_cursor,
    execute_ranked_pipeline_cursor,
    parse_search_cursor,
)
from ._engine import RankedPipelineSql
from ._materialize_hits import search_trust_source
from ._offset_run import RankedOffsetPlan, execute_simple_ranked_offset_search
from ._pgroonga_plan import is_coalesced_read_heap
from ._pipeline_sql import (
    PipelineAliases,
    build_rank_first_order,
    build_rank_select,
    scored_key_order,
)
from ._port import PostgresSearchPortMixin
from ._search_count import effective_search_count, resolve_ranked_approximate_total

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

    _index_qname_cell: OnceCell[PostgresQualifiedName] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )
    _index_heap_qname_cell: OnceCell[PostgresQualifiedName] = attrs.field(
        factory=OnceCell,
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

    read_validation: Literal["strict", "trusted"] = "strict"
    """Row decode mode for search hits (``trusted`` skips Pydantic validation)."""

    # ....................... #

    @property
    def search_capabilities(self) -> SearchCapabilities:
        # FTS / PGroonga rank over a full keyset cursor → bounded-memory export. The vector
        # subclass overrides this (top-k, no whole-corpus stream).
        return SearchCapabilities(supports_stream=True)

    # ....................... #

    async def _index_qname(self) -> PostgresQualifiedName:
        async def _factory() -> PostgresQualifiedName:
            return await resolve_postgres_qname(
                self.index_relation,
                self._tenant_id_for_resolve(),
            )

        return await self._index_qname_cell.resolve(
            _factory,
            cache=is_static_relation(self.index_relation),
        )

    # ....................... #

    async def _pipeline_read_qname(self) -> PostgresQualifiedName:
        """Read projection qname; honors ``read_relation`` when set on the adapter."""

        read = getattr(self, "read_relation", None)

        if read is not None:
            return await resolve_postgres_qname(read, self._tenant_id_for_resolve())

        return await self._qname()

    # ....................... #

    async def _pipeline_heap_qname(self) -> PostgresQualifiedName:
        """Index heap qname; honors ``heap_relation_spec`` when set on the adapter."""

        heap = getattr(self, "heap_relation_spec", None)

        if heap is not None:
            return await resolve_postgres_qname(heap, self._tenant_id_for_resolve())

        return await self._index_heap_qname()

    # ....................... #

    async def _index_heap_qname(self) -> PostgresQualifiedName:
        async def _factory() -> PostgresQualifiedName:
            return await resolve_postgres_qname(
                self.index_heap_relation,
                self._tenant_id_for_resolve(),
            )

        return await self._index_heap_qname_cell.resolve(
            _factory,
            cache=is_static_relation(self.index_heap_relation),
        )

    # ....................... #

    @property
    def index_qname(self) -> PostgresQualifiedName:
        """Best-effort sync access when :attr:`index_relation` is static."""

        resolved = self._index_qname_cell.peek()

        if resolved is not None:
            return resolved

        if is_static_relation(self.index_relation):
            return PostgresQualifiedName(*self.index_relation)

        raise exc.internal(
            "index_qname is only available for static index_relation; use await _index_qname()",
        )

    # ....................... #

    @property
    def index_heap_qname(self) -> PostgresQualifiedName:
        """Best-effort sync access when :attr:`index_heap_relation` is static."""

        resolved = self._index_heap_qname_cell.peek()

        if resolved is not None:
            return resolved

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
        for_cursor: bool = False,
        sorts: QuerySortExpression | None = None,  # type: ignore[valid-type]
    ) -> RankedPipelineSql:
        """Assemble pipeline CTEs; engine-specific leg SQL is built inside subclasses.

        *sorts* is the page order after the rank, which a capped CTE keeps (see
        :meth:`_capped_order`).

        ``for_cursor`` disables the ranked-candidate cap for keyword/text engines: cursor
        pagination walks the full ranked set one keyset page at a time, and the cap (a top-N
        bound for a single offset page, applied before the keyset seek) would otherwise
        truncate a deep cursor walk — and a bounded-memory stream export — at the cap. The
        top-k vector engine keeps its cap (its bound is the top-k, not an offset optimization).
        """

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

    def _read_heap_relation_specs(self) -> tuple[RelationSpec, RelationSpec]:
        read = getattr(self, "read_relation", None)
        heap = getattr(self, "heap_relation_spec", None)

        if read is None:
            read = self.relation

        if heap is None:
            heap = self.index_heap_relation

        return read, heap

    # ....................... #

    def _is_coalesced_read_heap_for(
        self,
        join_pairs: Sequence[tuple[str, str]] | None,
    ) -> bool:
        read, heap = self._read_heap_relation_specs()
        return is_coalesced_read_heap(read, heap, join_pairs)

    # ....................... #

    async def _coalesced_heap_where(
        self,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        *,
        parsed: QueryExpr | None,
        coalesced: bool,
    ) -> tuple[sql.Composable | None, list[Any]]:
        """The heap-side ``WHERE`` of a pipeline whose projection is the heap itself.

        Built whenever the projection is the heap, filters or not: that pipeline has no
        filtered CTE, so this clause is the only place the tenant predicate can go.
        """

        if not coalesced:
            return None, []

        return await self.where_clause(filters, parsed=parsed, table_alias=self.pipeline.index)

    # ....................... #

    async def _capped_order(
        self,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
        *,
        coalesced: bool,
        join_pairs: Sequence[tuple[str, str]],
    ) -> tuple[sql.Composable, sql.Composable | None]:
        """The page order after the rank, inside a capped CTE, and what the filtered CTE carries.

        A cap ordered by rank alone keeps an arbitrary few of the rows tying at its edge; ordered
        as the page is, it keeps exactly the page order's first rows, so an offset page cut from
        it matches the uncapped cursor. The key columns close the order where the sort does not.
        On a projection apart from the heap, the sort's columns ride the filtered CTE, which the
        capped CTE joins.
        """

        if not sorts:
            return sql.SQL(", ").join(scored_key_order(join_pairs)), None

        fields = set(sorts)

        if coalesced:
            # The heap is ordered by each field's own column.
            keys = scored_key_order(join_pairs, ordered=fields)
            on_heap = await self.order_by_clause(sorts, table_alias=self.pipeline.index)

            return sql.SQL(", ").join([on_heap, *keys]), None

        joined = {projection for projection, _ in join_pairs}
        roots = dict.fromkeys(field.split(".", 1)[0] for field in sorts)
        carried = [
            sql.SQL("{} AS {}").format(
                sql.Identifier(self.pipeline.projection, root), sql.Identifier(root)
            )
            for root in roots
            if root not in joined
        ]
        on_filtered = await self.order_by_clause(sorts, table_alias=self.pipeline.filtered)
        # The filtered CTE carries a key's projection column, which the join equates with the
        # key's heap column, so ordering by the one orders by the other.
        keys = scored_key_order(join_pairs, ordered={ic for pc, ic in join_pairs if pc in fields})

        return (
            sql.SQL(", ").join([on_filtered, *keys]),
            sql.SQL(", ").join(carried) if carried else None,
        )

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
        options = search_options_for_simple_adapter(options, spec=self.spec)
        parsed_filters = self.compile_filters(filters)
        fw, fp = await self.where_clause(filters, parsed=parsed_filters)
        terms = tuple(normalize_search_queries(query))
        page_order = resolve_search_sorts(
            sorts,
            default_sort=self.spec.default_sort,
            read_fields=self.read_fields,
            model=self.model_type,
            spec_name=self.spec.name,
        )
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
            sorts=page_order,
        )
        extra_ob = await self._projection_order_by_clause(page_order)
        order_sql = build_rank_first_order(
            aliases=self.pipeline,
            extra_order=extra_ob,
        )

        approximate_total: int | None = None
        count_policy = effective_search_count(options)

        if return_count and count_policy == "approximate" and not terms:
            proj_qname = await self._qname()
            approximate_total = await resolve_ranked_approximate_total(
                introspector=self.introspector,
                schema=proj_qname.schema,
                relation=proj_qname.name,
                where_sql=fw,
                params=fp,
            )

        plan = RankedOffsetPlan(
            with_clause=pipeline_sql.with_clause,
            from_outer=pipeline_sql.from_outer,
            order_sql=order_sql,
            params=pipeline_sql.params_body,
            count_params=pipeline_sql.count_params,
            count_with_clause=pipeline_sql.count_with_clause,
            count_from_outer=pipeline_sql.count_from_outer,
            approximate_total=approximate_total,
            select_table_alias=self.projection_alias,
            rank_select=build_rank_select(self.pipeline) if terms else None,
            highlight=pipeline_sql.highlight,
            from_outer_param_count=pipeline_sql.from_outer_param_count,
        )

        fp_extras = self._fingerprint_extras(
            options,
            resolved_plan=getattr(pipeline_sql, "resolved_plan", None),
            candidate_limit=getattr(pipeline_sql, "candidate_limit", None),
        )

        # Late materialization: when the read relation is a distinct projection
        # from the index heap (typically a heavy view), rank an id-only scan and hydrate the
        # page's read-model columns by id. No-op when read == heap (plain table search) or
        # when highlights are requested (snippets need the projection row in the SELECT).
        thin_read_qname: PostgresQualifiedName | None = None

        if pipeline_sql.highlight is None and ID_FIELD in self.read_fields:
            read_qn = await self._pipeline_read_qname()
            heap_qn = await self._pipeline_heap_qname()

            if (read_qn.schema, read_qn.name) != (heap_qn.schema, heap_qn.name):
                thin_read_qname = read_qn

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
            options=options,
            trust_source=search_trust_source(self.read_validation),
            thin_read_qname=thin_read_qname,
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
        reject_encrypted_sort_fields(
            sorts, encryption=self.spec.encryption, spec_name=self.spec.name
        )
        # Checked against the read model before anything is built from it, as offset pages are.
        page_order = resolve_search_sorts(
            sorts or self.spec.default_sort,
            default_sort=None,
            read_fields=self.read_fields,
            model=self.model_type,
            spec_name=self.spec.name,
        )
        # A null placement the seek cannot keep is refused here; offset pages honour it.
        refuse_cursor_null_placement(
            sorts,
            default_sort=self.spec.default_sort,
            read_fields=self.read_fields,
            backend="Postgres search cursor",
            sealed=self.sealed_fields,
        )
        options = search_options_for_simple_adapter(options, spec=self.spec)
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
                trust_source=search_trust_source(self.read_validation),
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
            for_cursor=True,
            sorts=page_order,
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
            trust_source=search_trust_source(self.read_validation),
        )
