from typing import Optional

from pydantic import Field

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.domain.models import BaseDTO

# ----------------------- #


class ListRequestDTO(BaseDTO):
    """List request payload for typed document list."""

    filters: Optional[QueryFilterExpression] = None  # type: ignore[valid-type]
    """Optional filter expression (predicates, conjunctions, disjunctions)."""

    sorts: Optional[QuerySortExpression] = None
    """Optional sort expression (field name to ``"asc"`` or ``"desc"``)."""


# ....................... #


class RawListRequestDTO(ListRequestDTO):
    """List request with required field projection for raw results.

    Extends :class:`ListRequestDTO` with :attr:`return_fields`. Backends
    return :class:`forze.base.primitives.JsonDict` hits instead of typed models.
    Requires at least one field in ``return_fields``.
    """

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""
