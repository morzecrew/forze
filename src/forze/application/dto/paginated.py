"""Paginated response DTOs for search and list operations.

Provides :class:`Paginated` (typed hits) and :class:`RawPaginated` (raw dict
hits). Page numbers are one-based; ``count`` is the total across all pages.
"""

from pydantic import BaseModel, PositiveInt

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO

# ----------------------- #


class Pagination(BaseDTO):
    """Pagination request payload."""

    page: PositiveInt = 1
    """One-based page number."""

    size: PositiveInt = 10
    """Page size (number of records per page)."""


# ....................... #


class Paginated[T: BaseModel](BaseDTO):
    """Paginated response with typed hit records.

    Used when search returns domain read models (e.g. :class:`ReadDocument`).
    ``page`` and ``size`` describe the requested slice; ``count`` is the
    total number of matching records.
    """

    hits: list[T]
    """Records for the current page."""

    page: int
    """One-based page number."""

    size: int
    """Page size (number of records per page)."""

    count: int
    """Total number of matching records across all pages."""


# ....................... #


class RawPaginated(BaseDTO):
    """Paginated response with raw dict hit records.

    Used when search returns field-projected JSON mappings instead of typed
    models. Same pagination semantics as :class:`Paginated`.
    """

    hits: list[JsonDict]
    """Raw record dicts for the current page."""

    page: int
    """One-based page number."""

    size: int
    """Page size (number of records per page)."""

    count: int
    """Total number of matching records across all pages."""
