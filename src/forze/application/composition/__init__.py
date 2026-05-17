"""Composition helpers built on top of the execution registry API."""

from forze.application.execution import (
    OperationNamespace,
    OperationRef,
    operation_namespace_for,
)

__all__ = [
    "OperationNamespace",
    "OperationRef",
    "operation_namespace_for",
]
