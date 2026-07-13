"""Mongo Atlas Vector Search adapter."""

from __future__ import annotations

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderPort
from forze.application.contracts.querying import QuerySortExpression
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    resolve_scoped_namespace,
)
from forze.application.contracts.search import SearchCapabilities, SearchOptions
from forze.base.primitives import OnceCell
from forze_mongo.adapters._logger import logger
from forze_mongo.kernel.relation import resolve_mongo_named_resource

from ._pipeline import build_browse_pipeline, build_vector_ranked_pipeline
from ._simple_base import MongoSimpleSearchAdapter

# ----------------------- #


def _embedding_query_text(terms: tuple[str, ...], *, combine: str) -> str:
    if not terms:
        return ""

    if combine == "all":
        return " ".join(terms)

    return terms[0] if len(terms) == 1 else " ".join(terms)


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoVectorSearchAdapter[M: BaseModel](MongoSimpleSearchAdapter[M]):
    """Semantic search using ``$vectorSearch`` (Atlas Vector Search)."""

    embedder: EmbeddingsProviderPort
    """Text-to-vector encoder for query strings."""

    embedding_dimensions: int
    """Expected embedding vector length."""

    vector_path: str
    """Document field holding the embedding array."""

    index_name: NamedResourceSpec
    """Atlas Vector Search index name (``$vectorSearch`` stage)."""

    _index_name_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    vector_num_candidates: int = attrs.field(default=100)
    """``numCandidates`` passed to ``$vectorSearch``."""

    vector_fetch_limit: int = attrs.field(default=100)
    """``$vectorSearch`` ``limit`` (must be ``<=`` :attr:`vector_num_candidates`)."""

    search_variant: str = attrs.field(default="mongo_vector", init=False)

    # ....................... #

    @property
    def search_capabilities(self) -> SearchCapabilities:
        # Atlas $vectorSearch: bring-your-own vector (embedder-encoded query); the stage
        # pre-filters on indexed fields before the ANN traversal.
        return SearchCapabilities(
            supports_vector=True,
            filtered_ann="prefilter",
            auto_embed=False,
        )

    # ....................... #

    async def _resolved_index_name(self) -> str:
        return await resolve_scoped_namespace(
            self.index_name,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._index_name_cell,
            resolver=resolve_mongo_named_resource,
        )

    # ....................... #

    async def _ranked_pipeline(
        self,
        *,
        terms: tuple[str, ...],
        combine: str,
        pre_filter: dict[str, Any],
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
    ) -> list[dict[str, Any]]:
        if not terms:
            return build_browse_pipeline(
                pre_filter=pre_filter,
                user_sorts=self._user_sorts(sorts),
                rank_field=self.rank_field,
            )

        text = _embedding_query_text(terms, combine=combine)

        if len(terms) > 1 and combine == "any":
            logger.warning(
                "mongo_vector_multi_phrase_any",
                message=(
                    "Vector search combines multiple phrases into one embedding; "
                    "use a single query string or phrase_combine='all' for stricter matching."
                ),
            )

        vec = await self.embedder.embed_one(text, input_kind="query")

        if len(vec) != self.embedding_dimensions:
            from forze.base.exceptions import exc

            raise exc.internal(
                f"Query embedding dimension {len(vec)} != configured {self.embedding_dimensions}."
            )

        return build_vector_ranked_pipeline(
            pre_filter=pre_filter,
            query_vector=vec,
            index_name=await self._resolved_index_name(),
            vector_path=self.vector_path,
            num_candidates=self._effective_num_candidates(options),
            limit=self.vector_fetch_limit,
            rank_field=self.rank_field,
            user_sorts=self._user_sorts(sorts),
        )

    # ....................... #

    def _effective_num_candidates(self, options: SearchOptions | None) -> int:
        """``numCandidates`` for ``$vectorSearch``: per-request ``max_candidates`` overrides the
        configured default. ``max_candidates`` is the advisory candidate-pool cap of
        :class:`~forze.application.contracts.search.SearchOptions`; for Atlas vector search it
        maps directly to ``numCandidates`` (the ANN breadth — a genuine recall/speed trade)."""

        raw = (options or {}).get("max_candidates")

        if raw is not None:
            return max(1, int(raw))

        return self.vector_num_candidates
