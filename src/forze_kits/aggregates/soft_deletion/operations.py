"""Soft-deletion operation kernel suffixes for usecase registration and resolution."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class SoftDeletionKernelOp(StrEnum):
    """Kernel segments (suffix only) for soft-deletion document usecase operation keys."""

    DELETE = "delete"
    """Soft-delete a document."""

    RESTORE = "restore"
    """Restore a soft-deleted document."""
