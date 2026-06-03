"""Soft-deletion composition: handlers, factories, and operation identifiers."""

from .factories import build_soft_deletion_registry
from .handlers import DeleteDocument, RestoreDocument
from .operations import SoftDeletionKernelOp

# ----------------------- #

__all__ = [
    "DeleteDocument",
    "RestoreDocument",
    "SoftDeletionKernelOp",
    "build_soft_deletion_registry",
]
