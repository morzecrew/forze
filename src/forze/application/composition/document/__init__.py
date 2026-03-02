"""Document composition: facades, factories, and operation identifiers.

Provides :class:`DocumentUsecasesFacade` (resolved usecases per operation),
:class:`DocumentUsecasesFacadeProvider` (factory for facades), and factories
for plans, mappers, and registries. Operations are identified by
:class:`DocumentOperation` enum.
"""

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
