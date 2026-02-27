from typing import Optional

from pydantic import Field

from forze.application.contracts.query import FilterExpression, SortExpression
from forze.domain.models import BaseDTO

# ----------------------- #


class SearchRequestDTO(BaseDTO):
    """Search request model."""

    query: str = ""
    """Query to search."""

    filters: Optional[FilterExpression] = None
    """Filters to search."""

    sorts: Optional[SortExpression] = None
    """Sort to search."""


# ....................... #


class RawSearchRequestDTO(SearchRequestDTO):
    """Raw search request model."""

    return_fields: set[str] = Field(min_length=1)
    """Return only these fields."""
