"""PGroonga search with projection vs index-heap separation (CTE pipeline)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Mapping, Sequence
from typing import Any, Final, Literal, final

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.querying import (
    PaginationExpression,
    QuerySortExpression,
    resolve_sort_keys,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchResultSnapshotOptions,
    SearchSpec,
    effective_phrase_combine,
)
from forze.base.exceptions import exc
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.relation import RelationSpec

from ._engine import RankedPipelineSql
from ._highlights import build_pgroonga_highlight
from ._leg_pgroonga import build_pgroonga_leg
from ._pgroonga_plan import (
    PgroongaPlan,
    effective_ranked_candidate_limit,
    ensure_pgroonga_plan_with_candidate_cap,
    index_first_heap_limit,
    is_coalesced_read_heap,
    is_trivial_filter,
    resolve_pgroonga_plan,
)
from ._pgroonga_sql import pgroonga_match_query_text, pgroonga_score_call
from ._pipeline_sql import (
    PipelineAliases,
    build_pgroonga_index_first_pipeline,
    outer_join_on_scored,
    scored_key_columns,
    scored_key_order,
    validate_join_pairs,
)
from ._ranked_pipeline import build_filter_first_ranked_pipeline, ranked_parts_to_sql
from ._search_count import effective_search_count
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

    def _heap_cap_order(
        self,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
        join: Sequence[tuple[str, str]],
    ) -> sql.Composable | None:
        """The page order after the rank on the heap alone, for an index-first cap.

        A sort key the heap carries, a join key or a mapped index field, reads its heap column;
        ``None`` when one is the projection's own, which the heap cannot order by.
        """

        on_heap = dict(join) | dict(self.index_field_map or {})
        parts: list[sql.Composable] = []
        ordered: list[str] = []

        for field, direction, nulls in resolve_sort_keys(sorts, sealed=self.sealed_fields):
            if (column := on_heap.get(field)) is None:
                return None

            part = sql.SQL("{} {}").format(
                sql.Identifier(self.pipeline.index, column),
                sql.SQL("ASC" if direction == "asc" else "DESC"),
            )

            # A record id is never null; another column takes the page's placement.
            if field != ID_FIELD:
                part = sql.SQL("{} {}").format(
                    part, sql.SQL("NULLS FIRST" if nulls == "first" else "NULLS LAST")
                )

            parts.append(part)
            ordered.append(column)

        return sql.SQL(", ").join([*parts, *scored_key_order(join, ordered=ordered)])

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
        for_cursor: bool = False,
        sorts: Any = None,
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

        read_spec = self.read_relation if self.read_relation is not None else self.relation
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
            parsed_filters=parsed_filters,
            read_qname=proj_qname,
            introspector=self.introspector,
            auto_index_first_min_rows=self.pgroonga_auto_index_first_min_rows,
            auto_filter_first_max_rows=self.pgroonga_auto_filter_first_max_rows,
            auto_with_filters=self.pgroonga_auto_with_filters,
            auto_use_exact_count=use_exact,
            count_filtered_rows=_count_filtered if use_exact else None,
            estimate_filtered_rows=(
                None if is_trivial_filter(parsed_filters) else _estimate_filtered
            ),
            tenant_aware=self.tenant_aware,
        )

        candidate_cap = effective_ranked_candidate_limit(
            # Cursor walks the whole ranked set; capping candidates would truncate a deep walk
            # / stream export. A ``None`` cap also switches an ``index_first`` plan to
            # ``filter_first`` below (index_first inherently caps via its heap ``LIMIT``).
            config_limit=None if for_cursor else self.pgroonga_candidate_limit,
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

        # Index-first caps the heap before it meets the projection, so its cap orders by what
        # the heap carries; a sort on a projection-only column runs filter-first, where it can.
        index_first_order: sql.Composable | None = None

        if resolved_plan == "index_first":
            if coalesced:
                index_first_order, _ = await self._capped_order(
                    sorts, coalesced=True, join_pairs=join
                )

            else:
                index_first_order = self._heap_cap_order(sorts, join)

            if index_first_order is None:
                resolved_plan = "filter_first"

        join_vs = outer_join_on_scored(
            join,
            projection_alias=self.pipeline.projection,
            scored_alias=self.pipeline.scored,
        )

        highlight = build_pgroonga_highlight(
            spec=self.spec,
            options=options,
            terms=terms,
            alias=self.projection_alias,
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
                scored_tiebreak=index_first_order,
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
                highlight=highlight,
                from_outer_param_count=len(fp),
            )

        cap_kw: dict[str, Any] = {}
        filtered_extra: sql.Composable | None = None

        if candidate_cap is not None:
            cap_kw = {
                "candidate_limit": candidate_cap,
                "scored_order": scored_order,
            }
            cap_kw["scored_tiebreak"], filtered_extra = await self._capped_order(
                sorts, coalesced=coalesced, join_pairs=join
            )

        heap_fw, heap_fp = await self._coalesced_heap_where(
            filters, parsed=parsed_filters, coalesced=coalesced
        )

        parts = build_filter_first_ranked_pipeline(
            aliases=self.pipeline,
            join_pairs=join,
            proj_ident=proj_qname.ident(),
            heap_ident=index_heap_qname.ident(),
            outer_proj_ident=(index_heap_qname.ident() if coalesced else proj_qname.ident()),
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
            filtered_extra=filtered_extra,
        )

        return ranked_parts_to_sql(
            parts,
            pipeline=self.pipeline,
            rank_column=self.search_rank_column,
            projection_alias=self.projection_alias,
            resolved_plan=resolved_plan,
            highlight=highlight,
        )
