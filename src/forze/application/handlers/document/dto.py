from uuid import UUID

from pydantic import BaseModel, Field

from forze.application.contracts.query import (
    AggregatesExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.dto.paginated import CursorPagination, Pagination
from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO

# ----------------------- #


class DocumentIdDTO(BaseDTO):
    """DTO for the document ID."""

    id: UUID
    """Document primary key."""


# ....................... #


class DocumentIdRevDTO(DocumentIdDTO):
    """DTO for the document ID and revision."""

    rev: int
    """Expected revision for optimistic concurrency."""


# ....................... #


class DocumentUpdateDTO[In: BaseDTO](DocumentIdRevDTO):
    """DTO for the document update."""

    dto: In
    """Update payload DTO."""


# ....................... #


class DocumentUpdateRes[Out: BaseModel](BaseDTO):
    """DTO for the document update response."""

    data: Out
    """Updated read model."""

    diff: JsonDict
    """Diff of the update."""


# ....................... #


class ListRequestDTO(Pagination):
    """List request payload for typed document list."""

    filters: QueryFilterExpression | None = None  # type: ignore[valid-type]
    """Optional filter expression (predicates, conjunctions, disjunctions)."""

    sorts: QuerySortExpression | None = None
    """Optional sort expression (field name to `"asc"` or `"desc"`)."""


# ....................... #


class ProjectedListRequestDTO(ListRequestDTO):
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


class ProjectedCursorListRequestDTO(CursorListRequestDTO):
    """Cursor list with required field projection for raw results."""

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""
