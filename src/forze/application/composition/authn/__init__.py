"""Authn composition: facades, factories, and operation identifiers."""

from forze.application.execution import OperationNamespace, OperationRef, operation_namespace_for
from .facades import AuthnUsecasesFacade
from .factories import build_authn_registry
from .operations import AuthnKernelOp

# ----------------------- #

__all__ = [
    "AuthnKernelOp",
    "OperationNamespace",
    "OperationRef",
    "operation_namespace_for",
    "AuthnUsecasesFacade",
    "build_authn_registry",
]
