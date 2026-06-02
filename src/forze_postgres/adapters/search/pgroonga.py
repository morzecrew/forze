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
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.relation import RelationSpec

from ._engine import RankedPipelineSql
from ._leg_pgroonga import build_pgroonga_leg
from ._materialize_hits import materialize_search_page
from ._pgroonga_plan import (
    effective_candidate_limit,
    is_coalesced_read_heap,
    is_trivial_filter,
    resolve_pgroonga_plan,
)
from ._pgroonga_sql import pgroonga_match_query_text, pgroonga_score_call
from ._pipeline_sql import (
    PipelineAliases,
    build_filtered_cte,
    build_outer_from,
    build_pgroonga_index_first_pipeline,
    build_pipeline_with_clause,
    build_scored_cte,
    filtered_select_list,
    outer_join_on_scored,
    scored_join_on_filtered,
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

        if return_count:
            total = int(
                await self.client.fetch_value(count_stmt, params_base, default=0),
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
            pool_pg0 = self.spec.resolved_read_codec.decode_mapping_many(rows)
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
        )

        if return_count:
            return page_from_limit_offset(
                page,
                pagination,
                total=total,
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
        index_heap_qname = await self._index_heap_qname()
        proj_qname = await self._qname()
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

        resolved_plan = await resolve_pgroonga_plan(
            configured=self.pgroonga_plan,
            options=options,
            parsed_filters=parsed_filters,
            read_qname=proj_qname,
            introspector=self.introspector,
            auto_index_first_min_rows=self.pgroonga_auto_index_first_min_rows,
            auto_use_exact_count=self.pgroonga_auto_use_exact_count,
            count_filtered_rows=(
                _count_filtered if self.pgroonga_auto_use_exact_count else None
            ),
        )

        candidate_cap = effective_candidate_limit(
            config_limit=self.pgroonga_candidate_limit,
            options=options,
            pagination=dict(pagination or {}),
            snapshot=snapshot,
            result_snapshot=self.result_snapshot,
            rs_spec=rs_spec,
        )

        join_vs = outer_join_on_scored(
            join,
            projection_alias=self.pipeline.projection,
            scored_alias=self.pipeline.scored,
        )

        if resolved_plan == "index_first":
            with_clause, from_outer = build_pgroonga_index_first_pipeline(
                aliases=self.pipeline,
                scored_keys=scored_keys,
                scored_rank=scored_rank,
                heap_ident=index_heap_qname.ident(),
                sw=sw,
                join_vs=join_vs,
                proj_ident=proj_qname.ident(),
                proj_fw=fw,
                candidate_limit=int(candidate_cap or 5000),
                scored_order=scored_order,
            )
            params_body = [*leg_params, *fp]

            return RankedPipelineSql(
                with_clause=with_clause,
                from_outer=from_outer,
                params_body=params_body,
                count_params=None,
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

        if coalesced:
            heap_fw: sql.Composable | None = None
            heap_fp: list[Any] = []

            if not is_trivial_filter(parsed_filters):
                heap_fw, heap_fp = await self.where_clause(
                    filters,
                    parsed=parsed_filters,
                    table_alias=self.pipeline.index,
                )

            scored_cte = build_scored_cte(
                aliases=self.pipeline,
                scored_keys=scored_keys,
                scored_rank=scored_rank,
                heap_ident=index_heap_qname.ident(),
                join_sf=None,
                sw=sw,
                heap_fw=heap_fw,
                first_in_with=True,
                **cap_kw,
            )
            with_clause = sql.SQL("WITH {}{}").format(scored_cte, sql.SQL(""))
            from_outer = build_outer_from(
                aliases=self.pipeline,
                proj_ident=index_heap_qname.ident(),
                join_vs=join_vs,
            )
            params_body = [*heap_fp, *leg_params]

        else:
            key_sel = filtered_select_list(
                join,
                projection_alias=self.pipeline.projection,
            )
            filtered_cte = build_filtered_cte(
                aliases=self.pipeline,
                key_sel=key_sel,
                proj_ident=proj_qname.ident(),
                fw=fw,
            )
            join_sf = scored_join_on_filtered(
                join,
                index_alias=self.pipeline.index,
                filtered_alias=self.pipeline.filtered,
            )
            scored_cte = build_scored_cte(
                aliases=self.pipeline,
                scored_keys=scored_keys,
                scored_rank=scored_rank,
                heap_ident=index_heap_qname.ident(),
                join_sf=join_sf,
                sw=sw,
                **cap_kw,
            )
            with_clause = build_pipeline_with_clause(filtered_cte, scored_cte)
            from_outer = build_outer_from(
                aliases=self.pipeline,
                proj_ident=proj_qname.ident(),
                join_vs=join_vs,
            )
            params_body = [*fp, *leg_params]

        return RankedPipelineSql(
            with_clause=with_clause,
            from_outer=from_outer,
            params_body=params_body,
            count_params=None,
            pipeline=self.pipeline,
            rank_column=self.search_rank_column,
            projection_alias=self.projection_alias,
            resolved_plan=resolved_plan,
            candidate_limit=candidate_cap,
        )
