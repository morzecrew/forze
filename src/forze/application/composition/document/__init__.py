"""Document composition: facades, factories, and operation identifiers."""

from .facades import DocumentDTOs, DocumentUsecasesFacade
from .factories import build_document_create_mapper, build_document_registry
from .operations import DocumentOperation

# ----------------------- #

__all__ = [
    "DocumentUsecasesFacade",
    "DocumentDTOs",
    "DocumentOperation",
    "build_document_create_mapper",
    "build_document_registry",
]
