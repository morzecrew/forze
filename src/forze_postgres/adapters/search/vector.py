"""Vector (pgvector) search with projection vs index-heap separation (CTE pipeline)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Final, Mapping, Sequence, final

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.embeddings import (
    EmbeddingsProviderPort,
    EmbeddingsSpec,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchSpec,
    effective_phrase_combine,
)
from forze.domain.constants import ID_FIELD

from ._engine import RankedPipelineSql
from ._leg_vector import build_vector_leg
from ._pipeline_sql import (
    PipelineAliases,
    build_filtered_cte,
    build_outer_from,
    build_pipeline_with_clause,
    build_scored_cte,
    filtered_select_list,
    outer_join_on_scored,
    scored_join_on_filtered,
    scored_key_columns,
    validate_join_pairs,
)
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

    search_variant: str = attrs.field(default="vector", init=False)
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

    def _fingerprint_extras(
        self,
        options: SearchOptions | None,
    ) -> dict[str, object] | None:
        return {
            "phrase_combine": str(effective_phrase_combine(options)),
            "embeddings": str(self.embeddings_spec.name),
            "vector_column": str(self.vector_column),
            "vector_distance": str(self.vector_distance),
            "embeddings_dim": int(self.embeddings_spec.dimensions),
        }

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
    ) -> RankedPipelineSql:
        _ = query, filters
        join = self._safe_join_pairs
        proj_qname = await self._qname()
        index_heap_qname = await self._index_heap_qname()

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
        params_body = [*fp, *leg_params]

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
        )
        join_vs = outer_join_on_scored(
            join,
            projection_alias=self.pipeline.projection,
            scored_alias=self.pipeline.scored,
        )
        from_outer = build_outer_from(
            aliases=self.pipeline,
            proj_ident=proj_qname.ident(),
            join_vs=join_vs,
        )
        with_clause = build_pipeline_with_clause(filtered_cte, scored_cte)

        return RankedPipelineSql(
            with_clause=with_clause,
            from_outer=from_outer,
            params_body=params_body,
            count_params=None,
            pipeline=self.pipeline,
            rank_column=self.search_rank_column,
            projection_alias=self.projection_alias,
        )
