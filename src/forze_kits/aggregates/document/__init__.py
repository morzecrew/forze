"""Document composition: facades, factories, and operation identifiers."""

from .dto import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    DocumentIdDTO,
    DocumentIdRevDTO,
    DocumentNumberIdDTO,
    DocumentUpdateDTO,
    DocumentUpdateRes,
    ListRequestDTO,
    ProjectedCursorListRequestDTO,
    ProjectedListRequestDTO,
)
from .facades import DocumentFacade
from .factories import build_document_registry
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
from .operations import DocumentKernelOp
from .value_objects import DocumentDTOs, DocumentMappers

# ----------------------- #

__all__ = [
    "DocumentDTOs",
    "DocumentFacade",
    "DocumentKernelOp",
    "DocumentMappers",
    "build_document_registry",
    "DocumentIdDTO",
    "DocumentIdRevDTO",
    "DocumentNumberIdDTO",
    "DocumentUpdateDTO",
    "DocumentUpdateRes",
    "CursorListRequestDTO",
    "ProjectedCursorListRequestDTO",
    "ListRequestDTO",
    "ProjectedListRequestDTO",
    "AggregatedListRequestDTO",
    "AggregatedListDocuments",
    "CreateDocument",
    "CursorListDocuments",
    "GetDocument",
    "KillDocument",
    "ListDocuments",
    "ProjectedCursorListDocuments",
    "ProjectedListDocuments",
    "UpdateDocument",
]
