"""Data transfer objects for application layer requests and responses."""

from .document import (
    DocumentIdDTO,
    DocumentIdRevDTO,
    DocumentNumberIdDTO,
    DocumentUpdateDTO,
    DocumentUpdateRes,
)
from .list_ import ListRequestDTO, RawListRequestDTO
from .paginated import Paginated, Pagination, RawPaginated
from .search import RawSearchRequestDTO, SearchRequestDTO
from .storage import ListObjectsRequestDTO, UploadObjectRequestDTO

# ----------------------- #

__all__ = [
    "Paginated",
    "RawPaginated",
    "Pagination",
    "SearchRequestDTO",
    "RawSearchRequestDTO",
    "ListRequestDTO",
    "RawListRequestDTO",
    "UploadObjectRequestDTO",
    "ListObjectsRequestDTO",
    "DocumentIdDTO",
    "DocumentIdRevDTO",
    "DocumentUpdateDTO",
    "DocumentNumberIdDTO",
    "DocumentUpdateRes",
]
