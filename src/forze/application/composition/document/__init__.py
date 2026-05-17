"""Document composition: facades, factories, and operation identifiers."""

from forze.application.execution import OperationNamespace, OperationRef, operation_namespace_for
from .facades import DocumentDTOs, DocumentUsecasesFacade
from .factories import (
    apply_default_tx_document_registry,
    build_document_create_mapper,
    build_document_list_cursor_mapper,
    build_document_list_mapper,
    build_document_raw_list_cursor_mapper,
    build_document_raw_list_mapper,
    build_document_registry,
    build_document_update_mapper,
)
from .operations import DocumentKernelOp

# ----------------------- #

__all__ = [
    "DocumentUsecasesFacade",
    "DocumentDTOs",
    "DocumentKernelOp",
    "OperationNamespace",
    "OperationRef",
    "operation_namespace_for",
    "build_document_create_mapper",
    "build_document_update_mapper",
    "build_document_registry",
    "build_document_list_mapper",
    "build_document_raw_list_mapper",
    "build_document_list_cursor_mapper",
    "build_document_raw_list_cursor_mapper",
    "apply_default_tx_document_registry",
]
