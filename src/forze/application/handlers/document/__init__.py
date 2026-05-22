"""Document handlers."""

from .dto import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    DocumentIdDTO,
    DocumentIdRevDTO,
    DocumentUpdateDTO,
    DocumentUpdateRes,
    ListRequestDTO,
    ProjectedCursorListRequestDTO,
    ProjectedListRequestDTO,
)
from .handlers import (
    AggregatedListDocuments,
    CreateDocument,
    CursorListDocuments,
    GetDocument,
    KillDocument,
    ListDocuments,
    ProjectedCursorListDocuments,
    ProjectedListDocuments,
    UpdateDocument,
)

# ----------------------- #

__all__ = [
    "CreateDocument",
    "KillDocument",
    "GetDocument",
    "UpdateDocument",
    "ListDocuments",
    "ProjectedListDocuments",
    "CursorListDocuments",
    "ProjectedCursorListDocuments",
    "AggregatedListDocuments",
    "DocumentIdDTO",
    "DocumentIdRevDTO",
    "DocumentUpdateDTO",
    "DocumentUpdateRes",
    "ListRequestDTO",
    "ProjectedListRequestDTO",
    "CursorListRequestDTO",
    "ProjectedCursorListRequestDTO",
    "AggregatedListRequestDTO",
]
