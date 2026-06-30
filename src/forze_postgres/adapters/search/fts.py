"""FTS search with projection vs index-heap separation (CTE pipeline)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Final, Mapping, Sequence, final

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchOptions,
    SearchSpec,
    effective_phrase_combine,
)
from ._search_count import effective_search_count
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.relation import RelationSpec

from ._engine import RankedPipelineSql
from ._highlights import build_fts_highlight
from ._pgroonga_plan import effective_ranked_candidate_limit, is_trivial_filter
from ._fts_sql import FtsGroupLetter
from ._leg_fts import build_fts_leg
from ._pipeline_sql import (
    PipelineAliases,
    scored_key_columns,
    scored_order_by_rank_alias,
    validate_join_pairs,
)
from ._ranked_pipeline import build_filter_first_ranked_pipeline, ranked_parts_to_sql
from ._simple_base import PostgresRankedPipelineSearchAdapter

# ----------------------- #

_DEFAULT_JOIN: Final[tuple[tuple[str, str], ...]] = ((ID_FIELD, ID_FIELD),)

_RANK_COLUMN: Final[str] = "_fts_rank"
_PIPELINE: Final[PipelineAliases] = PipelineAliases(rank_column=_RANK_COLUMN)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFTSSearchAdapter[M: BaseModel](PostgresRankedPipelineSearchAdapter[M]):
    """FTS :class:`SearchQueryPort` using a projection relation and index heap.

    Structured filters (and tenant scope) apply on the **projection** relation
    (:attr:`~PostgresGateway.qname`), typically a view. Matching and
    ``ts_rank_cd`` use the **index heap** (``await _index_heap_qname()``) and the
    ``tsvector`` expression from the index relation (``await _index_qname()``),
    mirroring :class:`PostgresPGroongaSearchAdapter`.
    """

    spec: SearchSpec[M]
    """Search specification."""

    fts_groups: dict[FtsGroupLetter, Sequence[str]]
    """Mapping of FTS weight letters to field names."""

    join_pairs: Sequence[tuple[str, str]] | None = attrs.field(default=None)
    """Join pairs (projection column, index heap column)."""

    index_field_map: Mapping[str, str] | None = attrs.field(default=None)
    """Reserved for API symmetry with PGroonga; FTS uses the catalog ``tsvector``."""

    ranked_candidate_limit: int | None = 5000
    """Cap ranked heap rows in the ``scored`` CTE; ``None`` disables."""

    read_relation: RelationSpec | None = attrs.field(default=None)
    heap_relation_spec: RelationSpec | None = attrs.field(default=None)

    search_variant: str = attrs.field(default="fts", init=False)
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
        **kwargs: object,
    ) -> dict[str, object] | None:
        extras: dict[str, object] = {
            "phrase_combine": str(effective_phrase_combine(options)),
            "search_count": str(effective_search_count(options)),
        }
        cap = kwargs.get("candidate_limit")
        if cap is not None:
            extras["candidate_limit"] = cap
        return extras

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
        pagination: Any = None,
        snapshot: Any = None,
        parsed_filters: Any = None,
    ) -> RankedPipelineSql:
        _ = query, filters
        join = self._safe_join_pairs
        index_qname = await self._index_qname()
        proj_qname = await self._pipeline_read_qname()
        index_heap_qname = await self._pipeline_heap_qname()
        rs_spec = self.spec.snapshot

        sw, scored_rank, leg_params = await build_fts_leg(
            introspector=self.introspector,
            index_qname=index_qname,
            search=self.spec,
            fts_groups=self.fts_groups,
            index_alias=self.pipeline.index,
            queries=terms,
            options=options,
            score_column=self.search_rank_column,
        )
        scored_keys = scored_key_columns(join, index_alias=self.pipeline.index)

        candidate_cap = effective_ranked_candidate_limit(
            config_limit=self.ranked_candidate_limit,
            options=options,
            pagination=dict(pagination or {}),
            snapshot=snapshot,
            result_snapshot=self.result_snapshot,
            rs_spec=rs_spec,
        )

        cap_kw: dict[str, Any] = {}

        if candidate_cap is not None:
            cap_kw = {
                "candidate_limit": candidate_cap,
                "scored_order": scored_order_by_rank_alias(self.search_rank_column),
            }

        coalesced = self._is_coalesced_read_heap_for(self.join_pairs)
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
            browse_count_params=[*fp] if not terms else None,
            highlight=build_fts_highlight(
                spec=self.spec,
                options=options,
                terms=terms,
                alias=self.projection_alias,
            ),
        )
