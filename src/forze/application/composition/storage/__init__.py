"""Storage composition: facades, factories, and operation identifiers."""

from .facades import StorageUsecasesFacade
from .factories import build_storage_registry
from .operations import StorageOperation

# ----------------------- #

__all__ = [
    "StorageUsecasesFacade",
    "StorageOperation",
    "build_storage_registry",
]
