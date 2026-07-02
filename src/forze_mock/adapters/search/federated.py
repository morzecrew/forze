"""In-memory federated search with weighted RRF merge."""

from __future__ import annotations

from typing import Any, Final, Sequence, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCountlessPage,
    SearchPage,
    search_page_from_limit_offset,
)
from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    FederatedSearchReadModel,
    FederatedSearchSpec,
    MultiSourceSearchOptions,
    SearchCapabilities,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    normalize_search_queries,
    prepare_federated_search_options,
    reject_federated_facets,
    resolve_fusion,
)
from forze.application.integrations.search import (
    SearchResultSnapshot,
    build_federated_highlight_index,
    federated_highlights_for_hits,
)
from forze_mock.adapters.search._unsupported import MockOffsetOnlySearchMixin
from forze_mock.adapters.search.query import MockSearchAdapter

# ----------------------- #

_DEFAULT_RRF_K: Final[int] = 60
_DEFAULT_PER_LEG_LIMIT: Final[int] = 5000


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockFederatedSearchAdapter[M: BaseModel](
    MockOffsetOnlySearchMixin[FederatedSearchReadModel[M]],
    SearchQueryPort[FederatedSearchReadModel[M]],
):
    """Federate mock search legs via :meth:`SearchResultSnapshot.weighted_rrf_merge_rows`."""

    federated_spec: FederatedSearchSpec[M]
    legs: Sequence[tuple[str, MockSearchAdapter[M]]]
    rrf_k: int = _DEFAULT_RRF_K
    rrf_per_leg_limit: int = _DEFAULT_PER_LEG_LIMIT
    result_snapshot: SearchResultSnapshot | None = None

    spec: FederatedSearchSpec[M] = attrs.field(
        default=attrs.Factory(lambda self: self.federated_spec, takes_self=True),
        init=False,
    )

    # ....................... #

    @property
    def search_capabilities(self) -> SearchCapabilities:
        # The reference adapter serves both fusion strategies (real backends currently
        # advertise only ``rrf`` and fail closed on ``weighted``).
        return SearchCapabilities(hybrid_fusion=frozenset({"rrf", "weighted"}))

    # ....................... #

    async def _merge_legs(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
    ) -> tuple[list[tuple[FederatedSearchReadModel[M], float]], dict[str, Any]]:
        """Run each leg, fuse (RRF or weighted relative-score), and index leg highlights.

        Returns the fused ``(hit, score)`` pairs in relevance order and the leg-highlight
        index. ``sorts`` order the fused set as tie-breakers under the fused score, mirroring
        the Postgres/Meilisearch federated adapters (and the thin executor)."""

        reject_federated_facets(options)
        fusion = resolve_fusion(
            cast("MultiSourceSearchOptions", options or {}).get("fusion"),
            self.search_capabilities,
            backend="mock_federated",
        )
        leg_opts, member_weights = prepare_federated_search_options(
            self.federated_spec,
            options,
        )
        leg_cap = max(1, int(self.rrf_per_leg_limit))
        rrf_rows: list[tuple[str, list[M], float]] = []
        weighted_rows: list[tuple[str, list[M], list[float], float]] = []
        leg_pages: list[tuple[str, Any]] = []

        for i, (name, port) in enumerate(self.legs):
            weight = float(member_weights[i])
            if weight <= 0.0:
                continue
            page = await port.search(
                query,
                filters,
                {"limit": leg_cap},
                None,
                options=leg_opts,
            )
            hits = list(page.hits)
            rrf_rows.append((name, hits, weight))
            leg_scores = page.scores if page.scores is not None else [1.0] * len(hits)
            weighted_rows.append((name, hits, list(leg_scores), weight))
            leg_pages.append((name, page))

        if fusion == "weighted":
            merged = SearchResultSnapshot.weighted_relative_merge_rows(
                leg_rows=weighted_rows,
            )
        else:
            merged = SearchResultSnapshot.weighted_rrf_merge_rows(
                leg_rows=rrf_rows,
                k=int(self.rrf_k),
            )

        SearchResultSnapshot.order_federated_full_merge(merged, sorts)
        return merged, build_federated_highlight_index(leg_pages)

    # ....................... #

    @staticmethod
    def _window(
        merged: list[tuple[FederatedSearchReadModel[M], float]],
        pagination: PaginationExpression | None,
    ) -> list[tuple[FederatedSearchReadModel[M], float]]:
        pg = pagination or {}
        offset = int(pg.get("offset") or 0)
        limit = pg.get("limit")
        window = merged[offset:]
        if limit is not None:
            window = window[: int(limit)]
        return window

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
    ) -> SearchCountlessPage[FederatedSearchReadModel[M]]:
        _ = snapshot
        merged, hl_index = await self._merge_legs(query, filters, sorts, options)
        window = self._window(merged, pagination)
        hits = [hit for hit, _ in window]
        scores = (
            [score for _, score in window]
            if normalize_search_queries(query)
            else None
        )
        highlights = federated_highlights_for_hits(hits, hl_index)
        return search_page_from_limit_offset(
            hits, pagination or {}, total=None, highlights=highlights, scores=scores
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
    ) -> SearchPage[FederatedSearchReadModel[M]]:
        _ = snapshot
        merged, hl_index = await self._merge_legs(query, filters, sorts, options)
        window = self._window(merged, pagination)
        hits = [hit for hit, _ in window]
        scores = (
            [score for _, score in window]
            if normalize_search_queries(query)
            else None
        )
        highlights = federated_highlights_for_hits(hits, hl_index)
        return search_page_from_limit_offset(
            hits, pagination or {}, total=len(merged), highlights=highlights, scores=scores
        )
