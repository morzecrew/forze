"""In-memory hub search over multiple mock search legs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, Page
from forze.application.contracts.querying import (
    AggregatesExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    refuse_unknown_keys,
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
    resolve_search_sorts,
    search_page_from_limit_offset,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD
from forze_mock.adapters.search._facets_highlights import (
    compute_facets,
    compute_highlights,
)
from forze_mock.adapters.search._unsupported import MockOffsetOnlySearchMixin
from forze_mock.adapters.search.query import (
    _MOCK_RANK,  # pyright: ignore[reportPrivateUsage]
    MockSearchAdapter,
    _without_mock_rank,  # pyright: ignore[reportPrivateUsage]
    check_mock_search_aggregate,
    filter_predicate,
    mock_aggregate_page,
)
from forze_mock.query.matching import (
    _project,  # pyright: ignore[reportPrivateUsage]
    _sort_docs,  # pyright: ignore[reportPrivateUsage]
)

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
        # is refused here rather than silently treated as the default merge. Aggregates read
        # every merged match, as the Postgres hub's do.
        return SearchCapabilities(hybrid_fusion=frozenset({"rrf"}), supports_aggregates=True)

    # ....................... #

    async def _aggregate_impl(
        self,
        aggregates: AggregatesExpression,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        *,
        options: SearchOptions | None,
        return_count: bool,
    ) -> CountlessPage[JsonDict] | Page[JsonDict]:
        """Group every merged hub match, the rows :meth:`search_page` counts."""

        parser = check_mock_search_aggregate(
            self.hub_spec, self.search_capabilities, aggregates, sorts, options=options
        )
        merged = await self._merged_docs(query, filters, None, options)

        return mock_aggregate_page(
            [doc for doc, _score in merged],
            self.hub_spec,
            aggregates,
            pagination,
            sorts,
            parser=parser,
            return_count=return_count,
        )

    async def aggregate_search(
        self,
        aggregates: AggregatesExpression,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._aggregate_impl(
            aggregates, query, filters, pagination, sorts, options=options, return_count=False
        )

    async def aggregate_search_page(
        self,
        aggregates: AggregatesExpression,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> Page[JsonDict]:
        page = await self._aggregate_impl(
            aggregates, query, filters, pagination, sorts, options=options, return_count=True
        )

        if not isinstance(page, Page):
            raise exc.internal("Mock hub search returned no total for a counted aggregate.")

        return page

    # ....................... #

    async def _merged_docs(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
    ) -> list[tuple[dict[str, Any], float]]:
        """Merge the legs and return ``(doc, hub_score)`` pairs in the page order.

        As the Postgres hub orders a page: each leg ranks its matches by relevance alone, ties
        by id; the merged rows then go by hub score, then by the request's sorts or the hub's
        own ``default_sort``, then by id.
        """

        order = resolve_search_sorts(
            sorts,
            default_sort=self.hub_spec.default_sort,
            read_fields=self.hub_spec.stored_read_fields,
            model=self.hub_spec.model_type,
            spec_name=self.hub_spec.name,
        )

        resolve_fusion(
            cast("MultiSourceSearchOptions", options or {}).get("fusion"),
            self.search_capabilities,
            backend="mock_hub",
        )
        leg_opts, weights = prepare_hub_search_options(self.hub_spec, options)
        # One parse, under the hub's own limits: the members' limits do not apply to it.
        matches = filter_predicate(filters, self.hub_spec.filter_limits)
        scores: dict[str, float] = {}
        docs: dict[str, dict[str, Any]] = {}

        for i, (_name, leg) in enumerate(self.legs):
            w = float(weights[i])
            if w <= 0.0:
                continue
            ordered = leg._full_ordered_search_documents(  # pyright: ignore[reportPrivateUsage]
                query,
                filters,
                {ID_FIELD: "asc"},
                leg_opts,
                matches=matches,
            )
            for rank, doc in enumerate(ordered, start=1):
                key = str(doc.get("id", rank))
                # The leg's own relevance, as the Postgres hub merges its engines' raw scores:
                # rows that tie in a leg tie in the hub, and the sort then orders them.
                contrib = w * float(doc[_MOCK_RANK])
                if key not in scores:
                    scores[key] = contrib
                    docs[key] = doc
                elif self.score_merge == "max":
                    if contrib > scores[key]:
                        scores[key] = contrib
                        docs[key] = doc
                else:
                    scores[key] += contrib

        # The sort is stable: by the hub order first, then by score, keeping it within a score.
        key_of = {id(doc): key for key, doc in docs.items()}
        by_order = _sort_docs(list(docs.values()), order)
        ranked = sorted(by_order, key=lambda doc: scores[key_of[id(doc)]], reverse=True)

        return [(doc, scores[key_of[id(doc)]]) for doc in ranked]

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

    async def _offset_page(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        pagination: PaginationExpression | None,
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
        snapshot: SearchResultSnapshotOptions | None,
        *,
        fields: Sequence[str] | None,
        return_count: bool,
    ) -> Any:
        """One merged offset page: typed hits, or the *fields* projection of each row."""

        refuse_unknown_keys(snapshot, SearchResultSnapshotOptions)
        ordered = await self._merged_docs(query, filters, sorts, options)
        window = self._window(ordered, pagination)
        page_docs = [doc for doc, _ in window]
        scores = [score for _, score in window] if normalize_search_queries(query) else None
        facets, highlights = self._facets_and_highlights(
            query, options, all_docs=[doc for doc, _ in ordered], page_docs=page_docs
        )
        hits: list[Any] = (
            [_project(_without_mock_rank(doc), fields) for doc in page_docs]
            if fields is not None
            else self._decode_hits(page_docs)
        )
        return search_page_from_limit_offset(
            hits,
            pagination or {},
            total=len(ordered) if return_count else None,
            facets=facets,
            highlights=highlights,
            scores=scores,
        )

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
        return await self._offset_page(
            query, filters, pagination, sorts, options, snapshot, fields=None, return_count=False
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
        return await self._offset_page(
            query, filters, pagination, sorts, options, snapshot, fields=None, return_count=True
        )

    async def project_search(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchCountlessPage[JsonDict]:
        return await self._offset_page(
            query, filters, pagination, sorts, options, snapshot, fields=fields, return_count=False
        )

    async def project_search_page(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchPage[JsonDict]:
        return await self._offset_page(
            query, filters, pagination, sorts, options, snapshot, fields=fields, return_count=True
        )
