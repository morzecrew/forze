"""Document composition: facades, factories, and operation identifiers."""

from .facades import DocumentFacade
from .factories import build_document_registry
from .operations import DocumentKernelOp
from .value_objects import DocumentDTOs, DocumentMappers

# ----------------------- #

__all__ = [
    "DocumentDTOs",
    "DocumentFacade",
    "DocumentKernelOp",
    "DocumentMappers",
    "build_document_registry",
]
