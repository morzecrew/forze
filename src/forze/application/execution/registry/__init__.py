"""Internal registry primitives for the execution subsystem."""

from ..registry_core import UsecaseRegistry
from .ops import OperationNamespace, OperationRef, operation_namespace_for

__all__ = [
    "OperationNamespace",
    "OperationRef",
    "UsecaseRegistry",
    "operation_namespace_for",
]
