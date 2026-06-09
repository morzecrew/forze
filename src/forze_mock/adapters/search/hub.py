"""In-memory hub search over multiple mock search legs."""

from __future__ import annotations

from typing import Any, Literal, Sequence, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, Page, page_from_limit_offset
from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    prepare_hub_search_options,
)
from forze.application.integrations.search import SearchResultSnapshot
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

    async def _merged_docs(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None,
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
    ) -> list[dict[str, Any]]:
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
        return [docs[k] for k in ranked]

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
    ) -> CountlessPage[M]:
        _ = snapshot
        ordered = await self._merged_docs(query, filters, sorts, options)
        pagination = pagination or {}
        limit = pagination.get("limit")
        offset = int(pagination.get("offset") or 0)
        page = ordered[offset:]
        if limit is not None:
            page = page[: int(limit)]
        allowed = set(self.hub_spec.model_type.model_fields.keys())
        typed = [{k: v for k, v in doc.items() if k in allowed} for doc in page]
        hits = self.hub_spec.resolved_read_codec.decode_mapping_many(typed)
        return page_from_limit_offset(hits, pagination, total=None)

    async def search_page(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Page[M]:
        _ = snapshot
        ordered = await self._merged_docs(query, filters, sorts, options)
        pagination = pagination or {}
        limit = pagination.get("limit")
        offset = int(pagination.get("offset") or 0)
        page = ordered[offset:]
        if limit is not None:
            page = page[: int(limit)]
        allowed = set(self.hub_spec.model_type.model_fields.keys())
        typed = [{k: v for k, v in doc.items() if k in allowed} for doc in page]
        hits = self.hub_spec.resolved_read_codec.decode_mapping_many(typed)
        return page_from_limit_offset(hits, pagination, total=len(ordered))
