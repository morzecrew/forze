"""Vector (pgvector) search with projection vs index-heap separation (CTE pipeline)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Mapping, Sequence
from typing import Any, Final, final

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.embeddings import (
    EmbeddingsProviderPort,
    EmbeddingsSpec,
)
from forze.application.contracts.search import (
    SearchCapabilities,
    SearchOptions,
    SearchSpec,
    effective_phrase_combine,
)
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.relation import RelationSpec

from ._engine import RankedPipelineSql
from ._leg_vector import build_vector_leg
from ._pgroonga_plan import effective_ranked_candidate_limit, is_trivial_filter
from ._pipeline_sql import (
    PipelineAliases,
    scored_key_columns,
    scored_order_by_rank_alias,
    validate_join_pairs,
)
from ._ranked_pipeline import build_filter_first_ranked_pipeline, ranked_parts_to_sql
from ._search_count import effective_search_count
from ._simple_base import PostgresRankedPipelineSearchAdapter
from ._vector_sql import VectorDistanceKind

# ----------------------- #

_DEFAULT_JOIN: Final[tuple[tuple[str, str], ...]] = ((ID_FIELD, ID_FIELD),)

_RANK_COLUMN: Final[str] = "_vector_rank"
_PIPELINE: Final[PipelineAliases] = PipelineAliases(rank_column=_RANK_COLUMN)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresVectorSearchAdapter[M: BaseModel](PostgresRankedPipelineSearchAdapter[M]):
    """pgvector :class:`SearchQueryPort`: KNN on a heap column with projection filters."""

    spec: SearchSpec[M]
    """Search specification."""

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

    ranked_candidate_limit: int | None = 5000
    """Cap ranked heap rows in the ``scored`` CTE; ``None`` disables."""

    read_relation: RelationSpec | None = attrs.field(default=None)
    heap_relation_spec: RelationSpec | None = attrs.field(default=None)

    search_variant: str = attrs.field(default="vector", init=False)
    pipeline: PipelineAliases = attrs.field(default=_PIPELINE, init=False)
    search_rank_column: str = attrs.field(default=_RANK_COLUMN, init=False)
    projection_alias: str = attrs.field(default="v", init=False)

    # ....................... #

    @property
    def search_capabilities(self) -> SearchCapabilities:
        # pgvector: bring-your-own vector (embedder-encoded query), filters applied around
        # the ANN scan so recall follows post-filter semantics under selective predicates.
        return SearchCapabilities(
            supports_vector=True,
            filtered_ann="postfilter",
            auto_embed=False,
        )

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
            "embeddings": str(self.embeddings_spec.name),
            "vector_column": str(self.vector_column),
            "vector_distance": str(self.vector_distance),
            "embeddings_dim": int(self.embeddings_spec.dimensions),
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
        for_cursor: bool = False,
    ) -> RankedPipelineSql:
        # Vector is top-k: its candidate cap is the retrieval bound, not an offset-page
        # optimization, so cursor pagination keeps it (unlike the keyword/text engines).
        _ = query, filters, for_cursor
        join = self._safe_join_pairs
        proj_qname = await self._pipeline_read_qname()
        index_heap_qname = await self._pipeline_heap_qname()
        rs_spec = self.spec.snapshot

        sw, scored_rank, leg_params = await build_vector_leg(
            embedder=self.embedder,
            introspector=self.introspector,
            index_alias=self.pipeline.index,
            vector_column=self.vector_column,
            vector_distance=self.vector_distance,
            embedding_dimensions=self.embeddings_spec.dimensions,
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
            candidate_order_asc=True,
            emit_exact_count_sql=bool(terms),
        )

        return ranked_parts_to_sql(
            parts,
            pipeline=self.pipeline,
            rank_column=self.search_rank_column,
            projection_alias=self.projection_alias,
            browse_count_params=[*fp] if not terms else None,
        )
