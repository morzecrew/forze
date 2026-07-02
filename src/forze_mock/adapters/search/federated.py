"""In-memory federated search with weighted RRF merge."""

from __future__ import annotations

from typing import Any, Final, Sequence, final

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
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    prepare_federated_search_options,
    reject_federated_facets,
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

    async def _merge_legs(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
    ) -> tuple[list[FederatedSearchReadModel[M]], dict[str, Any]]:
        """Run each leg (with facet/highlight options), RRF-merge, and index leg highlights.

        ``sorts`` order the fused set as tie-breakers under the RRF score, mirroring the
        Postgres/Meilisearch federated adapters (and the thin executor)."""

        reject_federated_facets(options)
        leg_opts, member_weights = prepare_federated_search_options(
            self.federated_spec,
            options,
        )
        leg_cap = max(1, int(self.rrf_per_leg_limit))
        leg_rows: list[tuple[str, list[M], float]] = []
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
            leg_rows.append((name, list(page.hits), weight))
            leg_pages.append((name, page))

        merged = SearchResultSnapshot.weighted_rrf_merge_rows(
            leg_rows=leg_rows,
            k=int(self.rrf_k),
        )
        SearchResultSnapshot.order_federated_full_merge(merged, sorts)
        hits = [item[0] for item in merged]
        return hits, build_federated_highlight_index(leg_pages)

    # ....................... #

    @staticmethod
    def _window(
        hits: list[FederatedSearchReadModel[M]],
        pagination: PaginationExpression | None,
    ) -> list[FederatedSearchReadModel[M]]:
        pg = pagination or {}
        offset = int(pg.get("offset") or 0)
        limit = pg.get("limit")
        window = hits[offset:]
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
        hits, hl_index = await self._merge_legs(query, filters, sorts, options)
        window = self._window(hits, pagination)
        highlights = federated_highlights_for_hits(window, hl_index)
        return search_page_from_limit_offset(
            window, pagination or {}, total=None, highlights=highlights
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
        hits, hl_index = await self._merge_legs(query, filters, sorts, options)
        window = self._window(hits, pagination)
        highlights = federated_highlights_for_hits(window, hl_index)
        return search_page_from_limit_offset(
            window, pagination or {}, total=len(hits), highlights=highlights
        )
