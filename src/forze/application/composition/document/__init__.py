"""Document composition: facades, factories, and operation identifiers."""

from .facades import (
    DocumentDTOSpec,
    DocumentUsecasesFacade,
    DocumentUsecasesModule,
)
from .factories import (
    build_document_create_mapper,
    build_document_registry,
    tx_document_plan,
)
from .operations import DocumentOperation

# ----------------------- #

__all__ = [
    "DocumentUsecasesFacade",
    "DocumentDTOSpec",
    "DocumentOperation",
    "build_document_create_mapper",
    "build_document_registry",
    "tx_document_plan",
    "DocumentUsecasesModule",
]
