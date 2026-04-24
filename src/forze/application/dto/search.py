from pydantic import Field

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import (
    SearchOptions,
    SearchResultSnapshotOptions,
)
from forze.domain.models import BaseDTO

from .paginated import CursorPagination, Pagination

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


class RawSearchRequestDTO(SearchRequestDTO):
    """Search request with required field projection for raw results.

    Extends `SearchRequestDTO` with `return_fields`.
    Backends return `JsonDict` hits instead of typed models.
    Requires at least one field in `return_fields`.
    """

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""


# ....................... #


class CursorSearchRequestDTO(CursorPagination, BaseSearchRequestDTO):
    """Cursor search request payload for typed document search."""


class RawCursorSearchRequestDTO(CursorSearchRequestDTO):
    """Cursor search request with required field projection for raw results."""

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""
