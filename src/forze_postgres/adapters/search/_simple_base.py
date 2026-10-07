"""Base class for projection + index-heap Postgres search adapters."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Sequence
from typing import Any, Literal

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, Page, page_from_limit_offset
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryExpr,
    QueryFilterExpression,
    QuerySortExpression,
    with_group_tiebreakers,
)
from forze.application.contracts.search import (
    SearchCapabilities,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    facet_size_of,
    normalize_search_queries,
    refuse_cursor_null_placement,
    resolve_facet_fields,
    resolve_search_sorts,
    search_options_for_simple_adapter,
    search_page_from_limit_offset,
)
from forze.application.integrations.document._limits import page_limit, page_offset
from forze.application.integrations.search import (
    SearchResultSnapshot,
    SnapshotWindow,
    build_snapshot_pool_streaming,
    decrypt_search_rows,
    reject_encrypted_sort_fields,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, OnceCell
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.relation import (
    RelationSpec,
    is_static_relation,
    resolve_postgres_qname,
)

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ...kernel.sql.query import PsycopgQueryRenderer, compose_aggregate_statement
from ._cursor_run import (
    execute_projection_keyset_cursor,
    execute_ranked_pipeline_cursor,
    parse_search_cursor,
)
from ._engine import RankedPipelineSql
from ._facets import fetch_pg_facets
from ._materialize_hits import materialize_search_page, search_trust_source
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
        # FTS / PGroonga rank over a full keyset cursor → bounded-memory export, and read the
        # whole matched set uncapped → aggregates. The vector subclass overrides this (top-k:
        # no whole-corpus stream, and every row "matches" an embedding).
        return SearchCapabilities(supports_stream=True, supports_aggregates=True)

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

        if not normalize_search_queries(query):
            return await self._offset_empty_query_browse(
                filters=filters,
                pagination=pagination,
                sorts=sorts,
                options=options,
                snapshot=snapshot,
                query=query,
                return_count=return_count,
                return_type=return_type,
                return_fields=return_fields,
            )

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

        plan = RankedOffsetPlan(
            with_clause=pipeline_sql.with_clause,
            from_outer=pipeline_sql.from_outer,
            order_sql=order_sql,
            params=pipeline_sql.params_body,
            count_params=pipeline_sql.count_params,
            count_with_clause=pipeline_sql.count_with_clause,
            count_from_outer=pipeline_sql.count_from_outer,
            approximate_total=None,
            select_table_alias=self.projection_alias,
            rank_select=build_rank_select(self.pipeline),
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

    async def _offset_empty_query_browse(
        self,
        *,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
        snapshot: SearchResultSnapshotOptions | None,
        return_count: bool,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
    ) -> Any:
        """A blank query: the read projection with filters only, as its cursor walks it.

        No term means no rank, so there is nothing for the ranked pipeline (and its join to
        the index heap) to add; reading the projection keeps the page, its count and the
        cursor on the same rows.
        """

        fw, fp = await self.where_clause(filters)
        rs_spec = self.spec.snapshot
        # Facets are computed live per page; an id-only snapshot replay would drop them, so a
        # facet request runs live (no snapshot read or write).
        facet_fields = resolve_facet_fields(self.spec, options)
        count_policy = effective_search_count(options)
        order = resolve_search_sorts(
            sorts,
            default_sort=self.spec.default_sort,
            read_fields=self.read_fields,
            model=self.model_type,
            spec_name=self.spec.name,
        )
        fp_fingerprint = SearchResultSnapshot.simple_search_fingerprint(
            query,
            filters,
            order,
            spec_name=self.spec.name,
            variant=self.search_variant,
            extras=self._fingerprint_extras(options),
        )

        if self.result_snapshot is not None and rs_spec is not None and not facet_fields:
            maybe_snap: Any = await self.result_snapshot.read_simple_result_snapshot(
                rs_spec=rs_spec,
                snap_opt=snapshot,
                fp_computed=fp_fingerprint,
                spec=self.spec,
                pagination=dict(pagination or {}),
                return_type=return_type,
                return_fields=return_fields,
                return_count=return_count and count_policy != "none",
            )

            if maybe_snap is not None:
                return maybe_snap

        # As the ranked page does, after a snapshot replay (which never re-sorts) had its
        # chance: ciphertext has no order at rest.
        reject_encrypted_sort_fields(
            sorts, encryption=self.spec.encryption, spec_name=self.spec.name
        )

        # A read model without an ``id`` and a request without a sort leave nothing to order
        # by but some column; the first field by name is at least the same one every time.
        order_sql = await self._projection_order_by_clause(
            order or {sorted(self.read_fields)[0]: "asc"}
        )
        proj_qname = await self._qname()
        count_stmt = sql.SQL(
            """
            SELECT COUNT(*) FROM {proj} {pa} WHERE {fw}
            """
        ).format(
            proj=proj_qname.ident(),
            pa=sql.Identifier(self.projection_alias),
            fw=fw,
        )

        params_base = list(fp)
        total = 0

        if return_count and count_policy != "none":
            if count_policy == "exact":
                total = int(
                    await self.client.fetch_value(count_stmt, params_base, default=0),
                )

                if total == 0:
                    # No matches: the facet distribution is empty buckets per requested field,
                    # not ``None`` — keep the sidecar shape the live path returns.
                    return search_page_from_limit_offset(  # pyright: ignore[reportUnknownVariableType]
                        [],
                        pagination or {},
                        total=0,
                        facets=dict.fromkeys(facet_fields, ()) if facet_fields else None,
                    )
            else:
                total = await resolve_ranked_approximate_total(
                    introspector=self.introspector,
                    schema=proj_qname.schema,
                    relation=proj_qname.name,
                    where_sql=fw,
                    params=params_base,
                )

        cols = self.return_clause(
            return_type,
            return_fields,
            table_alias=self.projection_alias,
        )
        data_stmt = sql.SQL(
            """
            SELECT {cols} FROM {proj} {pa} WHERE {fw} ORDER BY {order}
            """
        ).format(
            cols=cols,
            proj=proj_qname.ident(),
            pa=sql.Identifier(self.projection_alias),
            fw=fw,
            order=order_sql,
        )

        params = params_base
        pagination = pagination or {}
        trust_source = search_trust_source(self.read_validation)
        read_codec = self.spec.resolved_read_codec
        u_ = int(pagination.get("offset") or 0)

        want_sn = (
            self.result_snapshot is not None
            and rs_spec is not None
            and not facet_fields
            and self.result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
        )

        if want_sn and self.result_snapshot is not None and rs_spec is not None:
            # Stream the ordered pool window-by-window into the snapshot store so peak memory
            # is one chunk, never the whole (up to ``max_ids``) decoded pool at once.
            page_limit = SearchResultSnapshot.snapshot_pagination(True, 0, dict(pagination))[2]
            base_params = list(params_base)

            async def fetch_window(window_offset: int, window_limit: int) -> SnapshotWindow:
                stmt = data_stmt + sql.SQL(" LIMIT {} OFFSET {}").format(
                    sql.Placeholder(), sql.Placeholder()
                )
                window_rows = await self.client.fetch_all(
                    stmt,
                    [*base_params, int(window_limit), int(window_offset)],
                    row_factory="dict",
                )

                return SnapshotWindow(rows=[dict(row) for row in window_rows])

            async def decrypt_window_rows(
                raw_rows: list[JsonDict],
            ) -> tuple[list[JsonDict], Any]:
                return await decrypt_search_rows(read_codec, raw_rows)

            stream = await build_snapshot_pool_streaming(
                result_snapshot=self.result_snapshot,
                rs_spec=rs_spec,
                snap_opt=snapshot,
                fp_computed=fp_fingerprint,
                codec=read_codec,
                prepare_rows=decrypt_window_rows,
                fetch_window=fetch_window,
                page_offset=u_,
                page_limit=page_limit,
                trust_source=trust_source,
            )
            handle_no = stream.handle
            page_rows = stream.page_rows
            page_codec = stream.page_codec

        else:
            handle_no = None
            sql_limit, _, page_limit = SearchResultSnapshot.snapshot_pagination(
                False, 0, dict(pagination)
            )
            stmt = data_stmt

            # An unlimited browse fetches the whole filtered projection; the spec's
            # ``max_results`` caps it as it caps a ranked page (an explicit limit wins).
            if sql_limit is None and self.spec.max_results is not None:
                sql_limit = self.spec.max_results

            if sql_limit is not None:
                stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
                params.append(int(sql_limit))

            if pagination.get("offset") is not None:
                stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
                params.append(int(pagination.get("offset") or 0))

            fetched = await self.client.fetch_all(stmt, params, row_factory="dict")
            # Every search read decrypts sealed fields once, before any decode or projection.
            page_rows, page_codec = await decrypt_search_rows(
                read_codec, [dict(row) for row in fetched]
            )

        page = materialize_search_page(
            page_rows=page_rows,
            pool=None,
            u=u_,
            page_limit=page_limit,
            return_type=return_type,
            return_fields=return_fields,
            model_type=self.model_type,
            codec=page_codec,
            trust_source=trust_source,
        )

        facets = await self._browse_facets(proj_qname, fw, fp, options)

        return search_page_from_limit_offset(
            page,
            pagination,
            total=(total if (return_count and count_policy != "none") else None),
            snapshot=handle_no,
            facets=facets,
        )

    # ....................... #

    async def _browse_facets(
        self,
        proj_qname: Any,
        fw: sql.Composable,
        fp: Sequence[Any],
        options: SearchOptions | None,
    ) -> Any:
        """Facets for the empty-query browse: ``GROUP BY`` over the filtered projection."""

        facet_fields = resolve_facet_fields(self.spec, options)
        if not facet_fields:
            return None

        body = sql.SQL("FROM {proj} {pa} WHERE {fw}").format(
            proj=proj_qname.ident(),
            pa=sql.Identifier(self.projection_alias),
            fw=fw,
        )

        return await fetch_pg_facets(
            self.client,
            with_clause=None,
            body=body,
            params=list(fp),
            table_alias=self.projection_alias,
            fields=facet_fields,
            size=facet_size_of(options),
        )

    # ....................... #

    async def _aggregate_source(
        self,
        *,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        options: SearchOptions | None,
    ) -> tuple[sql.Composable | None, sql.Composable, list[Any]]:
        """The rows an exact page total counts: a ``WITH`` clause (or ``None``), a ``FROM``
        fragment over :attr:`projection_alias`, and their parameters.

        The page's own source: the ranked pipeline with no candidate cap, or for a blank
        query the projection the browse reads (:meth:`_offset_empty_query_browse`).
        """

        if not normalize_search_queries(query):
            fw, fp = await self.where_clause(filters)
            source = sql.SQL("FROM {proj} {pa} WHERE {fw}").format(
                proj=(await self._qname()).ident(),
                pa=sql.Identifier(self.projection_alias),
                fw=fw,
            )

            return None, source, list(fp)

        parsed_filters = self.compile_filters(filters)
        fw, fp = await self.where_clause(filters, parsed=parsed_filters)
        pipeline_sql = await self._build_ranked_pipeline_sql(
            query=query,
            filters=filters,
            options=options,
            fw=fw,
            fp=fp,
            terms=tuple(normalize_search_queries(query)),
            parsed_filters=parsed_filters,
            for_cursor=True,
        )

        return pipeline_sql.with_clause, pipeline_sql.from_outer, list(pipeline_sql.params_body)

    # ....................... #

    async def _aggregate_search_impl(
        self,
        aggregates: AggregatesExpression,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
        *,
        options: SearchOptions | None,
        return_count: bool,
    ) -> CountlessPage[JsonDict] | Page[JsonDict]:
        """Group the rows the search matches (:meth:`_aggregate_source`), read as a
        ``_matched`` CTE and aggregated as the document gateway aggregates its relation."""

        options = search_options_for_simple_adapter(options, spec=self.spec)
        with_clause, from_outer, source_params = await self._aggregate_source(
            query=query,
            filters=filters,
            options=options,
        )
        alias = sql.Identifier(self.projection_alias)
        matched = sql.SQL("_matched AS (SELECT {alias}.* {from_outer})").format(
            alias=alias,
            from_outer=from_outer,
        )
        head = (
            sql.SQL("WITH {matched}").format(matched=matched)
            if with_clause is None
            else sql.SQL("{with_clause}, {matched}").format(
                with_clause=with_clause,
                matched=matched,
            )
        )
        renderer = PsycopgQueryRenderer(
            types=await self.column_types(),
            model_type=self.model_type,
            nested_field_hints=self.nested_field_hints,
            table_alias=self.projection_alias,
        )
        aggregate = compose_aggregate_statement(
            renderer,
            aggregates,
            filter_parser=self.filter_parser,
            source=sql.SQL("FROM _matched AS {alias}").format(alias=alias),
            source_params=[],
        )
        params = [*source_params, *aggregate.params]
        window: dict[str, Any] = dict(pagination or {})
        limit, offset = page_limit(window), page_offset(window)
        # Rendered before anything runs, so a sort naming no output alias costs no query.
        # The group keys close the order, so pages of groups neither repeat nor skip one.
        order = PsycopgQueryRenderer.render_aggregate_order_by(
            aggregate.parsed,
            with_group_tiebreakers(aggregates, sorts),
        )
        total: int | None = None

        if return_count:
            count_stmt = sql.SQL("{head} SELECT COUNT(*) FROM ({inner}) AS _groups").format(
                head=head,
                inner=aggregate.stmt,
            )
            total = int(await self.client.fetch_value(count_stmt, params, default=0))

        stmt = sql.SQL("{head} {inner}").format(head=head, inner=aggregate.stmt)

        if order is not None:
            stmt += sql.SQL(" ORDER BY {order}").format(order=order)

        page_params = list(params)

        # Without a limit every group comes back, as the document port's aggregate drains them.
        if limit is not None:
            stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            page_params.append(limit)

        if offset:
            stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            page_params.append(offset)

        rows = await self.client.fetch_all(stmt, page_params, row_factory="dict")
        hits = [dict(row) for row in rows]

        if total is None:
            return page_from_limit_offset(hits, window)

        return page_from_limit_offset(hits, window, total=total)

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
