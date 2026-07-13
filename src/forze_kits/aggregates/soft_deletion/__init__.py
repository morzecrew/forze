"""Soft-deletion composition: handlers, factories, and operation identifiers."""

from .factories import build_soft_deletion_registry
from .handlers import DeleteDocument, RestoreDocument
from .operations import SoftDeletionKernelOp
from .wiring import (
    PurgeHook,
    SoftDeleteAwareGet,
    SoftDeleteWiring,
    exclude_soft_deleted_mapper,
    soft_delete_wiring,
)

# ----------------------- #

__all__ = [
    "DeleteDocument",
    "RestoreDocument",
    "SoftDeletionKernelOp",
    "build_soft_deletion_registry",
    "PurgeHook",
    "SoftDeleteAwareGet",
    "SoftDeleteWiring",
    "exclude_soft_deleted_mapper",
    "soft_delete_wiring",
]
