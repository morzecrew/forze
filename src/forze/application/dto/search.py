from pydantic import Field

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import SearchOptions

from .paginated import Pagination

# ----------------------- #


class SearchRequestDTO(Pagination):
    """Search request payload for typed document search.

    When `query` is non-empty, backends use full-text search (with fuzzy
    matching when enabled). When empty, only `filters` and `sorts`
    apply (filter-only mode).
    """

    query: str = ""
    """Full-text search query; empty string for filter-only mode."""

    filters: QueryFilterExpression | None = None  # type: ignore[valid-type]
    """Optional filter expression (predicates, conjunctions, disjunctions)."""

    sorts: QuerySortExpression | None = None
    """Optional sort expression (field name to `"asc"` or `"desc"`)."""

    options: SearchOptions | None = None
    """Optional search options."""


# ....................... #


class RawSearchRequestDTO(SearchRequestDTO):
    """Search request with required field projection for raw results.

    Extends `SearchRequestDTO` with `return_fields`.
    Backends return `JsonDict` hits instead of typed models.
    Requires at least one field in `return_fields`.
    """

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""
