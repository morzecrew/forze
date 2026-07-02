"""Search page value objects: base pagination plus result-level search metadata.

The base contract's :class:`~forze.application.contracts.base.CountlessPage` /
:class:`~forze.application.contracts.base.Page` /
:class:`~forze.application.contracts.base.CursorPage` are plain pagination containers used by
every read surface. The search surfaces return these subclasses, which add the optional
facet distributions, per-hit highlights, and the snapshot continuation handle — concepts that
only exist for search, so they do not weigh on document or analytics pages.
"""

from __future__ import annotations

from typing import Any, Mapping, overload

import attrs

from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
    offset_page_coords,
)

from .value_objects import FacetResults, HitHighlights, SearchSnapshotHandle

# ----------------------- #


# Search pages are non-slotted: ``SearchPage`` inherits both ``Page`` (for ``count``) and
# ``SearchCountlessPage`` (for the search metadata), and a slotted diamond over a shared base
# is a C-level layout conflict. The cost is a per-instance ``__dict__``, negligible for the
# one page object built per response, and it keeps ``SearchPage`` a true ``Page`` subtype so
# every ``Page``-typed helper accepts it unchanged.


@attrs.define(slots=False, kw_only=True, frozen=True)
class SearchCountlessPage[T](CountlessPage[T]):
    """A countless page carrying optional search result metadata."""

    snapshot: SearchSnapshotHandle | None = None
    """When present, a snapshot of ordered ids was used or created for this search."""

    facets: FacetResults | None = None
    """Optional facet (term) distributions for this search, when facets were requested;
    ``None`` when not requested."""

    highlights: list[HitHighlights] | None = None
    """Optional per-hit highlighted fragments, index-aligned with :attr:`hits`
    (``highlights[i]`` describes ``hits[i]``), when highlighting was requested. ``None`` when
    not requested or unavailable (e.g. snapshot-continuation pages)."""

    scores: list[float] | None = None
    """Optional per-hit relevance / similarity scores, index-aligned with :attr:`hits`
    (``scores[i]`` is the score of ``hits[i]``). Populated for ranked surfaces where a score is
    meaningful — the fused RRF score for federated results, the engine rank/similarity for
    vector and hybrid search. ``None`` when scoring is not meaningful (e.g. a filter-only
    browse) or not surfaced by the backend for this page (e.g. snapshot-continuation pages,
    where order is fixed and scores are not persisted). Higher is more relevant; the scale is
    backend- and strategy-specific (RRF scores in particular cluster near the top of the list),
    so treat scores as ordinal within one response rather than comparable across queries."""


# ....................... #


@attrs.define(slots=False, kw_only=True, frozen=True)
class SearchPage[T](Page[T], SearchCountlessPage[T]):
    """A counted search page: a base ``Page`` (with ``count``) plus the search metadata."""


# ....................... #


@attrs.define(slots=False, kw_only=True, frozen=True)
class SearchCursorPage[T](CursorPage[T]):
    """A cursor-paginated search page carrying optional facets / highlights.

    Cursor pages have no snapshot handle (snapshotting is an offset-pagination concern)."""

    facets: FacetResults | None = None
    """Optional facet (term) distributions for this search, when facets were requested."""

    highlights: list[HitHighlights] | None = None
    """Optional per-hit highlighted fragments, index-aligned with :attr:`hits`, when
    highlighting was requested. ``None`` when not requested or unavailable."""

    scores: list[float] | None = None
    """Optional per-hit relevance / similarity scores, index-aligned with :attr:`hits`.
    See :attr:`SearchCountlessPage.scores`."""


# ....................... #


@overload
def search_page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: None = None,
    snapshot: SearchSnapshotHandle | None = None,
    facets: FacetResults | None = None,
    highlights: list[HitHighlights] | None = None,
    scores: list[float] | None = None,
) -> SearchCountlessPage[T]: ...


@overload
def search_page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: int,
    snapshot: SearchSnapshotHandle | None = None,
    facets: FacetResults | None = None,
    highlights: list[HitHighlights] | None = None,
    scores: list[float] | None = None,
) -> SearchPage[T]: ...


def search_page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: int | None = None,
    snapshot: SearchSnapshotHandle | None = None,
    facets: FacetResults | None = None,
    highlights: list[HitHighlights] | None = None,
    scores: list[float] | None = None,
) -> SearchPage[T] | SearchCountlessPage[T]:
    """Build a ``SearchPage`` / ``SearchCountlessPage`` from offset/limit window params.

    The search counterpart to :func:`~forze.application.contracts.base.page_from_limit_offset`:
    same one-based page numbering, plus the optional snapshot handle / facets / highlights /
    per-hit scores.
    """

    page_num, size = offset_page_coords(pagination, len(hits))

    if total is None:
        return SearchCountlessPage(
            hits=hits,
            page=page_num,
            size=size,
            snapshot=snapshot,
            facets=facets,
            highlights=highlights,
            scores=scores,
        )

    return SearchPage(
        hits=hits,
        page=page_num,
        size=size,
        count=int(total),
        snapshot=snapshot,
        facets=facets,
        highlights=highlights,
        scores=scores,
    )
