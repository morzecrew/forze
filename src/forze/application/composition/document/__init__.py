"""Document composition: facades, factories, and operation identifiers."""

from .facades import DocumentDTOs, DocumentUsecasesFacade
from .factories import (
    build_document_create_mapper,
    build_document_list_cursor_mapper,
    build_document_list_mapper,
    build_document_raw_list_cursor_mapper,
    build_document_raw_list_mapper,
    build_document_registry,
    build_document_update_mapper,
)
from .operations import DocumentOperation

# ----------------------- #

__all__ = [
    "DocumentUsecasesFacade",
    "DocumentDTOs",
    "DocumentOperation",
    "build_document_create_mapper",
    "build_document_update_mapper",
    "build_document_registry",
    "build_document_list_mapper",
    "build_document_raw_list_mapper",
    "build_document_list_cursor_mapper",
    "build_document_raw_list_cursor_mapper",
]
