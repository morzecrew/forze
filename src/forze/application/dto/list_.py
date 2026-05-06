from pydantic import Field

from forze.application.contracts.query import (
    AggregatesExpression,
    QueryFilterExpression,
    QuerySortExpression,
)

from .paginated import CursorPagination, Pagination

# ----------------------- #


class ListRequestDTO(Pagination):
    """List request payload for typed document list."""

    filters: QueryFilterExpression | None = None  # type: ignore[valid-type]
    """Optional filter expression (predicates, conjunctions, disjunctions)."""

    sorts: QuerySortExpression | None = None
    """Optional sort expression (field name to `"asc"` or `"desc"`)."""


# ....................... #


class RawListRequestDTO(ListRequestDTO):
    """List request with required field projection for raw results.

    Extends `ListRequestDTO` with `return_fields`.
    Backends return `JsonDict` hits instead of typed models.
    Requires at least one field in `return_fields`.
    """

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""


# ....................... #


class AggregatedListRequestDTO(Pagination):
    """List request with aggregates expression."""

    aggregates: AggregatesExpression  # type: ignore[valid-type]
    """Aggregates expression."""

    filters: QueryFilterExpression | None = None  # type: ignore[valid-type]
    """Optional filter expression (predicates, conjunctions, disjunctions)."""

    sorts: QuerySortExpression | None = None
    """Optional sort expression (field name to `"asc"` or `"desc"`)."""


# ....................... #


class CursorListRequestDTO(CursorPagination):
    """List request for cursor (keyset) pagination."""

    filters: QueryFilterExpression | None = None  # type: ignore[valid-type]
    """Optional filter expression (predicates, conjunctions, disjunctions)."""

    sorts: QuerySortExpression | None = None
    """Optional sort expression (field name to `"asc"` or `"desc"`)."""


# ....................... #


class RawCursorListRequestDTO(CursorListRequestDTO):
    """Cursor list with required field projection for raw results."""

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""
