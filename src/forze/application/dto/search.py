"""Search request DTOs for document search usecases.

Provides :class:`SearchRequestDTO` (typed search) and :class:`RawSearchRequestDTO`
(field-projected search with required ``return_fields``). Used by
:class:`forze.application.usecases.document.SearchDocument` and
:class:`forze.application.usecases.document.RawSearchDocument`.
"""

from typing import Optional

from pydantic import Field

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import SearchOptions
from forze.domain.models import BaseDTO

# ----------------------- #


class SearchRequestDTO(BaseDTO):
    """Search request payload for typed document search.

    When :attr:`query` is non-empty, backends use full-text search (with fuzzy
    matching when enabled). When empty, only :attr:`filters` and :attr:`sorts`
    apply (filter-only mode).
    """

    query: str = ""
    """Full-text search query; empty string for filter-only mode."""

    filters: Optional[QueryFilterExpression] = None
    """Optional filter expression (predicates, conjunctions, disjunctions)."""

    sorts: Optional[QuerySortExpression] = None
    """Optional sort expression (field name to ``"asc"`` or ``"desc"``)."""

    options: Optional[SearchOptions] = None
    """Optional search options."""


# ....................... #


class RawSearchRequestDTO(SearchRequestDTO):
    """Search request with required field projection for raw results.

    Extends :class:`SearchRequestDTO` with :attr:`return_fields`. Backends
    return :class:`forze.base.primitives.JsonDict` hits instead of typed models.
    Requires at least one field in ``return_fields``.
    """

    return_fields: set[str] = Field(min_length=1)
    """Field names to project in the response; must not be empty."""
