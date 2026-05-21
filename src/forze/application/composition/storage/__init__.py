"""Storage composition: facades, factories, and operation identifiers."""

from .facades import StorageFacade
from .factories import build_storage_registry
from .operations import StorageKernelOp

# ----------------------- #

__all__ = [
    "StorageFacade",
    "StorageKernelOp",
    "build_storage_registry",
]
