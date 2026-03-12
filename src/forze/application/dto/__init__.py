"""Data transfer objects for application layer requests and responses.

Provides search request DTOs (:class:`SearchRequestDTO`, :class:`RawSearchRequestDTO`)
and paginated response DTOs (:class:`Paginated`, :class:`RawPaginated`). All extend
:class:`forze.domain.models.BaseDTO` for validation and serialization.
"""

from .list_ import ListRequestDTO, RawListRequestDTO
from .paginated import Paginated, Pagination, RawPaginated
from .search import RawSearchRequestDTO, SearchRequestDTO

# ----------------------- #

__all__ = [
    "Paginated",
    "RawPaginated",
    "Pagination",
    "SearchRequestDTO",
    "RawSearchRequestDTO",
    "ListRequestDTO",
    "RawListRequestDTO",
]
