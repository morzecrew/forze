from __future__ import annotations

from typing import Any, cast

from pydantic import BaseModel, Field

from forze.application.contracts.search import (
    FacetResults,
    HitHighlights,
    SearchCursorPage,
    SearchOptions,
    SearchPage,
    SearchResultSnapshotOptions,
    SearchSnapshotHandle,
)
from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO
from forze_kits.dto.paginated import (
    CursorPaginated,
    CursorPagination,
    Paginated,
    Pagination,
    ProjectedCursorPaginated,
    ProjectedPaginated,
    cursor_page_fields,
    offset_page_fields,
)
from forze_kits.dto.querying import (
    OptionalFilterExpression,
    OptionalSortExpression,
)

# ----------------------- #


class BaseSearchRequestDTO[O: SearchOptions = SearchOptions](BaseDTO):
    """Base search request payload, generic over the per-request options shape.

    ``O`` defaults to :class:`SearchOptions` for single-index search; hub and federated
    requests parametrize it with :class:`MultiSourceSearchOptions` so their ``options`` body
    also accepts the member-selection and merge-pool keys.
    """

    query: str | list[str] = ""
    """Full-text query string, or list of phrases (combined per ``phrase_combine``)."""

    filters: OptionalFilterExpression = None
    """Optional filter expression (predicates, conjunctions, disjunctions); a bare `{}` is treated as no filter."""

    sorts: OptionalSortExpression = None
    """Optional sort expression (field name to `"asc"` or `"desc"`); a bare `{}` is treated as no sort."""

    options: O | None = None
    """Optional search options."""

    snapshot: SearchResultSnapshotOptions | None = None
    """Optional result snapshot options."""


# ....................... #


class SearchRequestDTO[O: SearchOptions = SearchOptions](Pagination, BaseSearchRequestDTO[O]):
    """Search request payload for typed document search.

    When `query` is non-empty (or a non-empty list of phrases), backends run
    search (with fuzzy matching when enabled). For a list, use
    ``options["phrase_combine"]`` to choose **OR** (``"any"``, default) or **AND**
    (``"all"``) between phrases. When empty (or only blank list entries), only
    ``filters`` and ``sorts`` apply (filter-only mode).
    """


# ....................... #


class ProjectedSearchRequestDTO[O: SearchOptions = SearchOptions](SearchRequestDTO[O]):
    """Search request with required field projection for raw results

    Extends `SearchRequestDTO` with `return_fields`.
    Backends return `JsonDict` hits instead of typed models.
    Requires at least one field in `return_fields`.
    """

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""


# ....................... #


class CursorSearchRequestDTO[O: SearchOptions = SearchOptions](
    CursorPagination, BaseSearchRequestDTO[O]
):
    """Cursor search request payload for typed document search."""


# ....................... #


class ProjectedCursorSearchRequestDTO[O: SearchOptions = SearchOptions](CursorSearchRequestDTO[O]):
    """Cursor search request with required field projection for raw results."""

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""


# ....................... #


class SearchSnapshotHandleDTO(BaseDTO):
    """Thin response DTO for :class:`~forze.application.contracts.base.SearchSnapshotHandle`.

    Echo ``id`` and ``fingerprint`` in the next request under
    ``SearchOptions`` ``result_snapshot`` to continue from the KV snapshot.
    """

    id: str
    """Snapshot run id."""

    fingerprint: str
    """Request fingerprint; echo for :meth:`~forze.application.contracts.search.SearchResultSnapshotPort.get_id_range`."""

    total: int
    """Number of entries materialized in the snapshot (after cap)."""

    capped: bool = False
    """Whether the result set was truncated to ``max_ids`` when the snapshot was written."""

    expires_at: int | None = None
    """Unix timestamp (UTC seconds) when the snapshot expires and replay stops serving it,
    or ``None`` for a legacy run written before expiry was tracked. Read it to know how long
    ``id`` / ``fingerprint`` stay valid before the query must be re-run."""

    # ....................... #

    @classmethod
    def from_handle(
        cls,
        handle: SearchSnapshotHandle | None,
    ) -> SearchSnapshotHandleDTO | None:
        """Map a contract handle to DTO, or ``None``."""

        if handle is None:
            return None

        return cls(
            id=handle.id,
            fingerprint=handle.fingerprint,
            total=handle.total,
            capped=handle.capped,
            expires_at=handle.expires_at,
        )


# ....................... #


class FacetBucketDTO(BaseDTO):
    """One value in a facet distribution: a field value and its matching-document count."""

    value: Any
    """The field value (a scalar)."""

    count: int
    """Number of matching documents carrying this value."""


def _facets_to_dto(
    facets: FacetResults | None,
) -> dict[str, list[FacetBucketDTO]] | None:
    """Map contract facet results to response DTOs, or ``None`` when facets were not requested."""

    if facets is None:
        return None

    return {
        field: [FacetBucketDTO(value=b.value, count=b.count) for b in buckets]
        for field, buckets in facets.items()
    }


def _highlights_to_dto(
    highlights: list[HitHighlights] | None,
) -> list[dict[str, tuple[str, ...]]] | None:
    """Per-hit highlight fragments as plain dicts (index-aligned with hits), or ``None``."""

    if highlights is None:
        return None

    return [dict(hit) for hit in highlights]


# ....................... #


class SearchPaginated[T: BaseModel](Paginated[T]):
    """Paginated response for search operations."""

    snapshot: SearchSnapshotHandleDTO | None = None
    """When present, KV result snapshot metadata for paged follow-up (send back in request ``snapshot``)."""

    facets: dict[str, list[FacetBucketDTO]] | None = None
    """Per-field facet distributions over the full matching set, when facets were requested."""

    highlights: list[dict[str, tuple[str, ...]]] | None = None
    """Per-hit highlighted fragments, index-aligned with ``hits``, when highlighting was requested."""

    # ....................... #

    @classmethod
    def from_search_page[X: BaseModel](cls, page: SearchPage[X]) -> SearchPaginated[X]:
        out = cast(type[SearchPaginated[X]], cls)

        return out(
            **offset_page_fields(page),
            snapshot=SearchSnapshotHandleDTO.from_handle(page.snapshot),
            facets=_facets_to_dto(page.facets),
            highlights=_highlights_to_dto(page.highlights),
        )


# ....................... #


class ProjectedSearchPaginated(ProjectedPaginated):
    """Paginated response for search operations with field projection."""

    snapshot: SearchSnapshotHandleDTO | None = None
    """When present, KV result snapshot metadata for paged follow-up (send back in request ``snapshot``)."""

    facets: dict[str, list[FacetBucketDTO]] | None = None
    """Per-field facet distributions over the full matching set, when facets were requested."""

    highlights: list[dict[str, tuple[str, ...]]] | None = None
    """Per-hit highlighted fragments, index-aligned with ``hits``, when highlighting was requested."""

    # ....................... #

    @classmethod
    def from_search_page(cls, page: SearchPage[JsonDict]) -> ProjectedSearchPaginated:
        return cls(
            **offset_page_fields(page),
            snapshot=SearchSnapshotHandleDTO.from_handle(page.snapshot),
            facets=_facets_to_dto(page.facets),
            highlights=_highlights_to_dto(page.highlights),
        )


# ....................... #


class SearchCursorPaginated[T: BaseModel](CursorPaginated[T]):
    """Cursor-paginated response for search operations (facets / highlights, no snapshot)."""

    facets: dict[str, list[FacetBucketDTO]] | None = None
    """Per-field facet distributions over the full matching set, when facets were requested."""

    highlights: list[dict[str, tuple[str, ...]]] | None = None
    """Per-hit highlighted fragments, index-aligned with ``hits``, when highlighting was requested."""

    # ....................... #

    @classmethod
    def from_search_page[X: BaseModel](cls, page: SearchCursorPage[X]) -> SearchCursorPaginated[X]:
        out = cast(type[SearchCursorPaginated[X]], cls)

        return out(
            **cursor_page_fields(page),
            facets=_facets_to_dto(page.facets),
            highlights=_highlights_to_dto(page.highlights),
        )


# ....................... #


class ProjectedSearchCursorPaginated(ProjectedCursorPaginated):
    """Cursor-paginated search response with field projection (facets / highlights)."""

    facets: dict[str, list[FacetBucketDTO]] | None = None
    """Per-field facet distributions over the full matching set, when facets were requested."""

    highlights: list[dict[str, tuple[str, ...]]] | None = None
    """Per-hit highlighted fragments, index-aligned with ``hits``, when highlighting was requested."""

    # ....................... #

    @classmethod
    def from_search_page(cls, page: SearchCursorPage[JsonDict]) -> ProjectedSearchCursorPaginated:
        return cls(
            **cursor_page_fields(page),
            facets=_facets_to_dto(page.facets),
            highlights=_highlights_to_dto(page.highlights),
        )
