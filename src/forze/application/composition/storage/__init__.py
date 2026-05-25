"""Storage composition: facades, factories, and operation identifiers."""

from .catalog import STORAGE_OPERATIONS, StorageOperationEntry, StoragePreset
from .facades import StorageFacade
from .factories import build_storage_registry
from .operations import StorageKernelOp

# ----------------------- #

__all__ = [
    "STORAGE_OPERATIONS",
    "StorageFacade",
    "StorageKernelOp",
    "StorageOperationEntry",
    "StoragePreset",
    "build_storage_registry",
]
