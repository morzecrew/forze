"""Document composition: facades, factories, and operation identifiers."""

from .catalog import (
    DOCUMENT_OPERATIONS,
    DocumentOperationEntry,
    DocumentPreset,
    document_capability_allows,
)
from .facades import DocumentFacade
from .factories import build_document_registry
from .operations import DocumentKernelOp
from .value_objects import DocumentDTOs, DocumentMappers

# ----------------------- #

__all__ = [
    "DOCUMENT_OPERATIONS",
    "DocumentDTOs",
    "DocumentFacade",
    "DocumentKernelOp",
    "DocumentMappers",
    "DocumentOperationEntry",
    "DocumentPreset",
    "build_document_registry",
    "document_capability_allows",
]
