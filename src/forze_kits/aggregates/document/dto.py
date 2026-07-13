from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from forze.application.contracts.querying import (
    AggregatesExpression,
)
from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO
from forze_kits.dto.paginated import CursorPagination, Pagination
from forze_kits.dto.querying import (
    OptionalFilterExpression,
    OptionalSortExpression,
)

# ----------------------- #


class DocumentIdDTO(BaseDTO):
    """DTO for the document ID."""

    id: UUID
    """Document primary key."""


# ....................... #


class DocumentNumberIdDTO(BaseDTO):
    """DTO for the document number ID."""

    number_id: int
    """Numeric document identifier."""


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


def written_read_model(result: Any) -> Any:
    """The read model a document write op produced.

    ``CREATE`` returns the read model directly; ``UPDATE`` wraps it as :attr:`DocumentUpdateRes.data`
    (alongside the diff). Shared so index sync and invariant enforcement unwrap a write result the
    same way.
    """

    return (  # pyright: ignore[reportUnknownVariableType]
        result.data  # pyright: ignore[reportUnknownMemberType]
        if isinstance(result, DocumentUpdateRes)
        else result
    )


# ....................... #


class ListRequestDTO(Pagination):
    """List request payload for typed document list."""

    filters: OptionalFilterExpression = None
    """Optional filter expression (predicates, conjunctions, disjunctions); a bare `{}` is treated as no filter."""

    sorts: OptionalSortExpression = None
    """Optional sort expression (field name to `"asc"` or `"desc"`); a bare `{}` is treated as no sort."""


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

    filters: OptionalFilterExpression = None
    """Optional filter expression (predicates, conjunctions, disjunctions); a bare `{}` is treated as no filter."""

    sorts: OptionalSortExpression = None
    """Optional sort expression (field name to `"asc"` or `"desc"`); a bare `{}` is treated as no sort."""


# ....................... #


class CursorListRequestDTO(CursorPagination):
    """List request for cursor (keyset) pagination."""

    filters: OptionalFilterExpression = None
    """Optional filter expression (predicates, conjunctions, disjunctions); a bare `{}` is treated as no filter."""

    sorts: OptionalSortExpression = None
    """Optional sort expression (field name to `"asc"` or `"desc"`); a bare `{}` is treated as no sort."""


# ....................... #


class ProjectedCursorListRequestDTO(CursorListRequestDTO):
    """Cursor list with required field projection for raw results."""

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""
