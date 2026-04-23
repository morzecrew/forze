"""Data transfer objects for application layer requests and responses."""

from .document import (
    DocumentIdDTO,
    DocumentIdRevDTO,
    DocumentNumberIdDTO,
    DocumentUpdateDTO,
    DocumentUpdateRes,
)
from .list_ import (
    CursorListRequestDTO,
    ListRequestDTO,
    RawCursorListRequestDTO,
    RawListRequestDTO,
)
from .paginated import (
    CursorPaginated,
    CursorPagination,
    Paginated,
    Pagination,
    RawCursorPaginated,
    RawPaginated,
)
from .search import (
    CursorSearchRequestDTO,
    RawCursorSearchRequestDTO,
    RawSearchRequestDTO,
    SearchRequestDTO,
)
from .storage import ListObjectsRequestDTO, UploadObjectRequestDTO

# ----------------------- #

__all__ = [
    "CursorPaginated",
    "RawCursorPaginated",
    "Paginated",
    "RawPaginated",
    "Pagination",
    "SearchRequestDTO",
    "RawSearchRequestDTO",
    "CursorSearchRequestDTO",
    "RawCursorSearchRequestDTO",
    "ListRequestDTO",
    "RawListRequestDTO",
    "CursorListRequestDTO",
    "RawCursorListRequestDTO",
    "UploadObjectRequestDTO",
    "ListObjectsRequestDTO",
    "DocumentIdDTO",
    "DocumentIdRevDTO",
    "DocumentUpdateDTO",
    "DocumentNumberIdDTO",
    "DocumentUpdateRes",
    "CursorPagination",
]
