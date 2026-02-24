from typing import Optional

from pydantic import Field

from forze.application.kernel.ports import DocumentSorts
from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO

# ----------------------- #


class SearchRequestDTO(BaseDTO):
    """Search request model."""

    query: str = ""
    """Query to search."""

    filters: Optional[JsonDict] = None
    """Filters to search."""

    sorts: Optional[DocumentSorts] = None
    """Sort to search."""


# ....................... #


class RawSearchRequestDTO(SearchRequestDTO):
    """Raw search request model."""

    return_fields: set[str] = Field(min_length=1)
    """Return only these fields."""
