"""Storage composition: facades, factories, and operation identifiers."""

from forze.application.execution import OperationNamespace, OperationRef, operation_namespace_for
from .facades import StorageUsecasesFacade
from .factories import build_storage_registry
from .operations import StorageKernelOp

# ----------------------- #

__all__ = [
    "StorageUsecasesFacade",
    "StorageKernelOp",
    "OperationNamespace",
    "OperationRef",
    "operation_namespace_for",
    "build_storage_registry",
]
