"""PGroonga search with projection vs index-heap separation (CTE pipeline)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Final, Literal, Mapping, Sequence, final

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.base import page_from_limit_offset
from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    PgroongaPlan,
    SearchOptions,
    SearchResultSnapshotOptions,
    SearchSpec,
    effective_phrase_combine,
    normalize_search_queries,
    search_options_for_simple_adapter,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import exc
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.relation import RelationSpec

from ._engine import RankedPipelineSql
from ._leg_pgroonga import build_pgroonga_leg
from ._materialize_hits import materialize_search_page, search_trust_source
from ._pgroonga_plan import (
    effective_ranked_candidate_limit,
    ensure_pgroonga_plan_with_candidate_cap,
    index_first_heap_limit,
    is_coalesced_read_heap,
    is_trivial_filter,
    resolve_pgroonga_plan,
)
from ._ranked_pipeline import build_filter_first_ranked_pipeline, ranked_parts_to_sql
from ._search_count import effective_search_count, resolve_ranked_approximate_total
from ._pgroonga_sql import pgroonga_match_query_text, pgroonga_score_call
from ._pipeline_sql import (
    PipelineAliases,
    build_pgroonga_index_first_pipeline,
    outer_join_on_scored,
    scored_key_columns,
    validate_join_pairs,
)
from ._simple_base import PostgresRankedPipelineSearchAdapter

# ----------------------- #

_DEFAULT_JOIN: Final[tuple[tuple[str, str], ...]] = ((ID_FIELD, ID_FIELD),)

_RANK_COLUMN: Final[str] = "_pgroonga_rank"
_PIPELINE: Final[PipelineAliases] = PipelineAliases(rank_column=_RANK_COLUMN)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresPGroongaSearchAdapter[M: BaseModel](
    PostgresRankedPipelineSearchAdapter[M],
):
    """PGroonga :class:`SearchQueryPort` using a projection relation and index heap."""

    spec: SearchSpec[M]
    """Search specification."""

    join_pairs: Sequence[tuple[str, str]] | None = attrs.field(default=None)
    """Join pairs (projection column, index heap column)."""

    index_field_map: Mapping[str, str] | None = attrs.field(default=None)
    """Index field map (projection column -> index heap column)."""

    pgroonga_score_version: Literal["v1", "v2"] = "v2"
    """``pgroonga_score`` form (``v1`` heap alias vs ``v2`` tableoid/ctid)."""

    pgroonga_plan: PgroongaPlan = "filter_first"
    """Ranked search SQL plan (``filter_first``, ``index_first``, ``auto``)."""

    pgroonga_candidate_limit: int | None = 5000
    """Default cap on ranked heap rows; ``None`` disables."""

    pgroonga_auto_index_first_min_rows: int = 100_000
    """``auto`` plan: ``index_first`` when read estimate is at least this size."""

    pgroonga_auto_use_exact_count: bool = False
    """``auto`` plan: use ``COUNT(*)`` on filtered projection to pick the plan."""

    pgroonga_auto_with_filters: bool = True
    """``auto`` plan: consider index-first when filters are eligible and estimates allow."""

    pgroonga_auto_filter_first_max_rows: int = 50_000
    """``auto`` with filters: prefer ``filter_first`` when filtered estimate is at most this size."""

    pgroonga_index_first_filter_margin: float = 3.0
    """Inflate heap top-K when index-first post-filters on the projection."""

    read_relation: RelationSpec | None = attrs.field(default=None)
    """Read relation spec (for coalesced read/heap detection)."""

    heap_relation_spec: RelationSpec | None = attrs.field(default=None)
    """Heap relation spec (for coalesced read/heap detection)."""

    search_variant: str = attrs.field(default="pgroonga", init=False)
    pipeline: PipelineAliases = attrs.field(default=_PIPELINE, init=False)
    search_rank_column: str = attrs.field(default=_RANK_COLUMN, init=False)
    projection_alias: str = attrs.field(default="v", init=False)

    # ....................... #

    @property
    def _safe_join_pairs(self) -> Sequence[tuple[str, str]]:
        return self.join_pairs or _DEFAULT_JOIN

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        validate_join_pairs(self._safe_join_pairs)

    # ....................... #

    def _fingerprint_extras(  # type: ignore[override]
        self,
        options: SearchOptions | None,
        *,
        resolved_plan: str | None = None,
        candidate_limit: int | None = None,
    ) -> dict[str, object] | None:
        extras: dict[str, object] = {
            "phrase_combine": str(effective_phrase_combine(options)),
            "search_count": str(effective_search_count(options)),
        }

        if resolved_plan is not None:
            extras["pgroonga_plan"] = resolved_plan

        if candidate_limit is not None:
            extras["candidate_limit"] = candidate_limit

        return extras

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
        terms = normalize_search_queries(query)

        if not terms:
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

        return await super()._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
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
        """Browse projection with filters only (PGroonga-specific snapshot fingerprint)."""

        fw, fp = await self.where_clause(filters)
        rs_spec = self.spec.snapshot
        fp_fingerprint = SearchResultSnapshot.simple_search_fingerprint(
            query,
            filters,
            sorts,
            spec_name=self.spec.name,
            variant=self.search_variant,
            extras=self._fingerprint_extras(options),
        )

        if self.result_snapshot is not None and rs_spec is not None:
            maybe_snap: Any = await self.result_snapshot.read_simple_result_snapshot(
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

        extra_ob = await self._projection_order_by_clause(sorts)
        order_parts: list[sql.Composable] = (  # type: ignore[assignment]
            [extra_ob]
            if extra_ob is not None
            else [
                sql.SQL("{} ASC").format(
                    sql.Identifier(
                        self.projection_alias,
                        sorted(self.read_fields)[0],
                    ),
                ),
            ]
        )
        order_sql = sql.SQL(", ").join(order_parts)
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
        count_policy = effective_search_count(options)

        if return_count and count_policy != "none":
            if count_policy == "exact":
                total = int(
                    await self.client.fetch_value(count_stmt, params_base, default=0),
                )

                if total == 0:
                    return page_from_limit_offset(  # pyright: ignore[reportUnknownVariableType]
                        [],
                        pagination or {},
                        total=0,
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

        want_sn = (
            self.result_snapshot is not None
            and rs_spec is not None
            and self.result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
        )
        max_n0 = (
            self.result_snapshot.effective_snapshot_max_ids(snapshot, rs_spec)
            if want_sn and self.result_snapshot is not None
            else 0
        )
        sql_limit, sql_offset, page_limit = SearchResultSnapshot.snapshot_pagination(
            want_sn, max_n0, dict(pagination)
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

        handle_no = None
        pool_pg0: list[M] | None = None
        u_ = int(pagination.get("offset") or 0)

        if want_sn and self.result_snapshot is not None and rs_spec is not None:
            pool_len = len(rows)
            pool_pg0 = self.spec.resolved_read_codec.decode_mapping_many(
                rows,
                trust_source=search_trust_source(self.read_validation),
            )
            handle_no = await self.result_snapshot.put_simple_ordered_hits(
                pool_pg0,
                snap_opt=snapshot,
                rs_spec=rs_spec,
                fp_computed=fp_fingerprint,
                pool_len_before_cap=pool_len,
            )
            rows = rows[u_ : u_ + page_limit]

        page = materialize_search_page(
            page_rows=rows,
            pool=pool_pg0,
            u=u_,
            page_limit=page_limit,
            return_type=return_type,
            return_fields=return_fields,
            model_type=self.model_type,
            codec=self.spec.resolved_read_codec,
            trust_source=search_trust_source(self.read_validation),
        )

        if return_count:
            return page_from_limit_offset(
                page,
                pagination,
                total=total if count_policy != "none" else None,
                snapshot=handle_no,
            )

        return page_from_limit_offset(
            page,
            pagination,
            total=None,
            snapshot=handle_no,
        )

    # ....................... #

    async def _build_ranked_pipeline_sql(
        self,
        *,
        query: str | Sequence[str],
        filters: Any,
        options: SearchOptions | None,
        fw: sql.Composable,
        fp: list[Any],
        terms: tuple[str, ...],
        pagination: PaginationExpression | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        parsed_filters: Any = None,
    ) -> RankedPipelineSql:
        _ = query, filters
        join = self._safe_join_pairs
        index_qname = await self._index_qname()
        proj_qname = await self._pipeline_read_qname()
        index_heap_qname = await self._pipeline_heap_qname()
        rs_spec = self.spec.snapshot

        mq = pgroonga_match_query_text(terms, options)

        sw, scored_rank, leg_params = await build_pgroonga_leg(
            introspector=self.introspector,
            index_qname=index_qname,
            search=self.spec,
            index_field_map=self.index_field_map,
            index_alias=self.pipeline.index,
            queries=terms,
            options=options,
            score_column=self.search_rank_column,
            pgroonga_score_version=self.pgroonga_score_version,
        )
        scored_keys = scored_key_columns(join, index_alias=self.pipeline.index)
        scored_order = pgroonga_score_call(
            index_alias=self.pipeline.index,
            query=mq,
            score_version=self.pgroonga_score_version,
        )

        read_spec = (
            self.read_relation if self.read_relation is not None else self.relation
        )
        heap_spec = (
            self.heap_relation_spec
            if self.heap_relation_spec is not None
            else self.index_heap_relation
        )
        coalesced = is_coalesced_read_heap(read_spec, heap_spec, self.join_pairs)

        async def _count_filtered() -> int:
            count_stmt = sql.SQL("SELECT COUNT(*) FROM {proj} {pa} WHERE {fw}").format(
                proj=proj_qname.ident(),
                pa=sql.Identifier(self.pipeline.projection),
                fw=fw,
            )
            return int(await self.client.fetch_value(count_stmt, list(fp), default=0))

        async def _estimate_filtered() -> int:
            return await self.introspector.estimate_filtered_rows(
                schema=proj_qname.schema,
                relation=proj_qname.name,
                where_sql=fw,
                params=fp,
            )

        use_exact = self.pgroonga_auto_use_exact_count and not is_trivial_filter(
            parsed_filters,
        )

        resolved_plan = await resolve_pgroonga_plan(
            configured=self.pgroonga_plan,
            options=options,
            parsed_filters=parsed_filters,
            read_qname=proj_qname,
            introspector=self.introspector,
            auto_index_first_min_rows=self.pgroonga_auto_index_first_min_rows,
            auto_filter_first_max_rows=self.pgroonga_auto_filter_first_max_rows,
            auto_with_filters=self.pgroonga_auto_with_filters,
            auto_use_exact_count=use_exact,
            count_filtered_rows=_count_filtered if use_exact else None,
            estimate_filtered_rows=(
                _estimate_filtered
                if not is_trivial_filter(parsed_filters)
                else None
            ),
            tenant_aware=self.tenant_aware,
        )

        candidate_cap = effective_ranked_candidate_limit(
            config_limit=self.pgroonga_candidate_limit,
            options=options,
            pagination=dict(pagination or {}),
            snapshot=snapshot,
            result_snapshot=self.result_snapshot,
            rs_spec=rs_spec,
        )

        resolved_plan = ensure_pgroonga_plan_with_candidate_cap(
            resolved_plan,
            candidate_cap,
        )

        join_vs = outer_join_on_scored(
            join,
            projection_alias=self.pipeline.projection,
            scored_alias=self.pipeline.scored,
        )

        if resolved_plan == "index_first":
            if candidate_cap is None:
                raise exc.internal("candidate_cap is None")

            heap_limit = index_first_heap_limit(
                int(candidate_cap),
                has_projection_filters=not is_trivial_filter(parsed_filters),
                filter_margin=self.pgroonga_index_first_filter_margin,
            )

            with_clause, from_outer = build_pgroonga_index_first_pipeline(
                aliases=self.pipeline,
                scored_keys=scored_keys,
                scored_rank=scored_rank,
                heap_ident=index_heap_qname.ident(),
                sw=sw,
                join_vs=join_vs,
                proj_ident=proj_qname.ident(),
                proj_fw=fw,
                heap_row_limit=heap_limit,
                scored_order=scored_order,
            )
            count_with, count_from = build_pgroonga_index_first_pipeline(
                aliases=self.pipeline,
                scored_keys=scored_keys,
                scored_rank=scored_rank,
                heap_ident=index_heap_qname.ident(),
                sw=sw,
                join_vs=join_vs,
                proj_ident=proj_qname.ident(),
                proj_fw=fw,
                heap_row_limit=None,
                scored_order=None,
            )
            params_body = [*leg_params, *fp]

            return RankedPipelineSql(
                with_clause=with_clause,
                from_outer=from_outer,
                params_body=params_body,
                count_params=list(params_body),
                count_with_clause=count_with,
                count_from_outer=count_from,
                pipeline=self.pipeline,
                rank_column=self.search_rank_column,
                projection_alias=self.projection_alias,
                resolved_plan=resolved_plan,
                candidate_limit=candidate_cap,
            )

        cap_kw: dict[str, Any] = {}

        if candidate_cap is not None:
            cap_kw = {
                "candidate_limit": candidate_cap,
                "scored_order": scored_order,
            }

        heap_fw: sql.Composable | None = None
        heap_fp: list[Any] = []

        if coalesced and not is_trivial_filter(parsed_filters):
            heap_fw, heap_fp = await self.where_clause(
                filters,
                parsed=parsed_filters,
                table_alias=self.pipeline.index,
            )

        parts = build_filter_first_ranked_pipeline(
            aliases=self.pipeline,
            join_pairs=join,
            proj_ident=proj_qname.ident(),
            heap_ident=index_heap_qname.ident(),
            outer_proj_ident=(
                index_heap_qname.ident() if coalesced else proj_qname.ident()
            ),
            fw=fw,
            fp=fp,
            leg_params=leg_params,
            sw=sw,
            scored_rank=scored_rank,
            scored_keys=scored_keys,
            coalesced=coalesced,
            heap_fw=heap_fw,
            heap_fp=heap_fp,
            cap_kw=cap_kw,
            emit_exact_count_sql=bool(terms),
        )

        return ranked_parts_to_sql(
            parts,
            pipeline=self.pipeline,
            rank_column=self.search_rank_column,
            projection_alias=self.projection_alias,
            resolved_plan=resolved_plan,
        )
