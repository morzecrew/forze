"""Document composition: facades, factories, and operation identifiers."""

from .facades import (
    DocumentDTOSpec,
    DocumentUsecasesFacade,
    DocumentUsecasesFacadeProvider,
)
from .factories import (
    build_document_create_mapper,
    build_document_plan,
    build_document_registry,
)
from .operations import DocumentOperation

# ----------------------- #

__all__ = [
    "DocumentUsecasesFacade",
    "DocumentUsecasesFacadeProvider",
    "DocumentDTOSpec",
    "DocumentOperation",
    "build_document_create_mapper",
    "build_document_plan",
    "build_document_registry",
]
