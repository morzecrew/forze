"""In-memory hub search over multiple mock search legs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    HubSearchSpec,
    MultiSourceSearchOptions,
    SearchCapabilities,
    SearchCountlessPage,
    SearchOptions,
    SearchPage,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    highlight_fragment_bounds,
    normalize_search_queries,
    prepare_hub_search_options,
    resolve_facet_fields,
    resolve_fusion,
    resolve_highlight,
    search_page_from_limit_offset,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze_mock.adapters.search._facets_highlights import (
    compute_facets,
    compute_highlights,
)
from forze_mock.adapters.search._unsupported import MockOffsetOnlySearchMixin
from forze_mock.adapters.search.query import MockSearchAdapter

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockHubSearchAdapter[M: BaseModel](
    MockOffsetOnlySearchMixin[M],
    SearchQueryPort[M],
):
    """Merge ranked results from multiple :class:`MockSearchAdapter` legs."""

    hub_spec: HubSearchSpec[M]
    legs: Sequence[tuple[str, MockSearchAdapter[M]]]
    combine: Literal["or", "and"] = "or"
    score_merge: Literal["max", "sum"] = "max"
    result_snapshot: SearchResultSnapshot | None = None

    spec: HubSearchSpec[M] = attrs.field(
        default=attrs.Factory(lambda self: self.hub_spec, takes_self=True),
        init=False,
    )

    # ....................... #

    @property
    def search_capabilities(self) -> SearchCapabilities:
        # Single-store hybrid: the hub's own rank-based leg merge (score_merge) is the
        # ``rrf`` fusion family. Weighted relative-score fusion is a federated concept and
        # is refused here rather than silently treated as the default merge.
        return SearchCapabilities(hybrid_fusion=frozenset({"rrf"}))

    # ....................... #

    async def _merged_docs(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
    ) -> list[tuple[dict[str, Any], float]]:
        """Merge the legs and return ``(doc, hub_score)`` pairs in descending score order."""

        resolve_fusion(
            cast("MultiSourceSearchOptions", options or {}).get("fusion"),
            self.search_capabilities,
            backend="mock_hub",
        )
        leg_opts, weights = prepare_hub_search_options(self.hub_spec, options)
        scores: dict[str, float] = {}
        docs: dict[str, dict[str, Any]] = {}

        for i, (_name, leg) in enumerate(self.legs):
            w = float(weights[i])
            if w <= 0.0:
                continue
            ordered = leg._full_ordered_search_documents(  # pyright: ignore[reportPrivateUsage]
                query,
                filters,
                sorts,
                leg_opts,
            )
            for rank, doc in enumerate(ordered, start=1):
                key = str(doc.get("id", rank))
                leg_score = 1.0 / float(rank)
                contrib = w * leg_score
                if key not in scores:
                    scores[key] = contrib
                    docs[key] = doc
                elif self.score_merge == "max":
                    if contrib > scores[key]:
                        scores[key] = contrib
                        docs[key] = doc
                else:
                    scores[key] += contrib

        ranked = sorted(scores.keys(), key=lambda k: scores[k], reverse=True)
        return [(docs[k], scores[k]) for k in ranked]

    # ....................... #

    def _facets_and_highlights(
        self,
        query: str | Sequence[str],
        options: SearchOptions | None,
        *,
        all_docs: Sequence[dict[str, Any]],
        page_docs: Sequence[dict[str, Any]],
    ) -> tuple[Any | None, list[Any] | None]:
        """Hub facets over the merged matched set + per-hit highlights over the page rows.

        Homogeneous hub → a flat :class:`FacetResults` (deduped exact, not per-leg summed)
        and whole-hub-row highlights; both keyed by the hub model fields."""

        facet_fields = resolve_facet_fields(self.hub_spec, options)
        facets = compute_facets(all_docs, facet_fields, options=options) if facet_fields else None

        highlight = resolve_highlight(self.hub_spec, options)
        fragment_size, max_fragments = highlight_fragment_bounds(options)
        highlights = (
            compute_highlights(
                page_docs,
                normalize_search_queries(query),
                highlight[0],
                pre_tag=highlight[1],
                post_tag=highlight[2],
                fragment_size=fragment_size,
                max_fragments=max_fragments,
            )
            if highlight is not None
            else None
        )

        return facets, highlights

    # ....................... #

    def _window(
        self,
        ordered: list[tuple[dict[str, Any], float]],
        pagination: PaginationExpression | None,
    ) -> list[tuple[dict[str, Any], float]]:
        pg = pagination or {}
        limit = pg.get("limit")
        offset = int(pg.get("offset") or 0)
        page = ordered[offset:]
        if limit is not None:
            page = page[: int(limit)]
        return page

    # ....................... #

    def _decode_hits(self, page_docs: Sequence[dict[str, Any]]) -> list[M]:
        allowed = set(self.hub_spec.model_type.model_fields.keys())
        typed = [{k: v for k, v in doc.items() if k in allowed} for doc in page_docs]
        return self.hub_spec.resolved_read_codec.decode_mapping_many(typed)

    # ....................... #

    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchCountlessPage[M]:
        _ = snapshot
        ordered = await self._merged_docs(query, filters, sorts, options)
        window = self._window(ordered, pagination)
        page_docs = [doc for doc, _ in window]
        scores = [score for _, score in window] if normalize_search_queries(query) else None
        facets, highlights = self._facets_and_highlights(
            query, options, all_docs=[doc for doc, _ in ordered], page_docs=page_docs
        )
        return search_page_from_limit_offset(
            self._decode_hits(page_docs),
            pagination or {},
            total=None,
            facets=facets,
            highlights=highlights,
            scores=scores,
        )

    async def search_page(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchPage[M]:
        _ = snapshot
        ordered = await self._merged_docs(query, filters, sorts, options)
        window = self._window(ordered, pagination)
        page_docs = [doc for doc, _ in window]
        scores = [score for _, score in window] if normalize_search_queries(query) else None
        facets, highlights = self._facets_and_highlights(
            query, options, all_docs=[doc for doc, _ in ordered], page_docs=page_docs
        )
        return search_page_from_limit_offset(
            self._decode_hits(page_docs),
            pagination or {},
            total=len(ordered),
            facets=facets,
            highlights=highlights,
            scores=scores,
        )
