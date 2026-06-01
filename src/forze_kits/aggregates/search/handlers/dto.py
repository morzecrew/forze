from __future__ import annotations

from typing import cast

from pydantic import BaseModel, Field

from forze.application.contracts.base import Page, SearchSnapshotHandle
from forze.application.contracts.querying import (
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchResultSnapshotOptions,
)
from forze_kits.dto.paginated import (
    CursorPagination,
    Paginated,
    Pagination,
    ProjectedPaginated,
)
from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO

# ----------------------- #


class BaseSearchRequestDTO(BaseDTO):
    """Base search request payload."""

    query: str | list[str] = ""
    """Full-text query string, or list of phrases (combined per ``phrase_combine``)."""

    filters: QueryFilterExpression | None = None  # type: ignore[valid-type]
    """Optional filter expression (predicates, conjunctions, disjunctions)."""

    sorts: QuerySortExpression | None = None
    """Optional sort expression (field name to `"asc"` or `"desc"`)."""

    options: SearchOptions | None = None
    """Optional search options."""

    snapshot: SearchResultSnapshotOptions | None = None
    """Optional result snapshot options."""


# ....................... #


class SearchRequestDTO(Pagination, BaseSearchRequestDTO):
    """Search request payload for typed document search.

    When `query` is non-empty (or a non-empty list of phrases), backends run
    search (with fuzzy matching when enabled). For a list, use
    ``options["phrase_combine"]`` to choose **OR** (``"any"``, default) or **AND**
    (``"all"``) between phrases. When empty (or only blank list entries), only
    ``filters`` and ``sorts`` apply (filter-only mode).
    """


# ....................... #


class ProjectedSearchRequestDTO(SearchRequestDTO):
    """Search request with required field projection for raw results

    Extends `SearchRequestDTO` with `return_fields`.
    Backends return `JsonDict` hits instead of typed models.
    Requires at least one field in `return_fields`.
    """

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""


# ....................... #


class CursorSearchRequestDTO(CursorPagination, BaseSearchRequestDTO):
    """Cursor search request payload for typed document search."""


# ....................... #


class ProjectedCursorSearchRequestDTO(CursorSearchRequestDTO):
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
        )


# ....................... #


class SearchPaginated[T: BaseModel](Paginated[T]):
    """Paginated response for search operations."""

    snapshot: SearchSnapshotHandleDTO | None = None
    """When present, KV result snapshot metadata for paged follow-up (send back in request ``snapshot``)."""

    # ....................... #

    @classmethod
    def from_page[X: BaseModel](cls, page: Page[X]) -> SearchPaginated[X]:
        out = cast(type[SearchPaginated[X]], cls)

        return out(
            hits=page.hits,
            page=page.page,
            size=page.size,
            count=page.count,
            snapshot=SearchSnapshotHandleDTO.from_handle(page.snapshot),
        )


# ....................... #


class ProjectedSearchPaginated(ProjectedPaginated):
    """Paginated response for search operations with field projection."""

    snapshot: SearchSnapshotHandleDTO | None = None
    """When present, KV result snapshot metadata for paged follow-up (send back in request ``snapshot``)."""

    # ....................... #

    @classmethod
    def from_page(cls, page: Page[JsonDict]) -> ProjectedSearchPaginated:
        return cls(
            hits=page.hits,
            page=page.page,
            size=page.size,
            count=page.count,
            snapshot=SearchSnapshotHandleDTO.from_handle(page.snapshot),
        )
