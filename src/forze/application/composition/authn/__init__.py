"""Authn composition: facades, factories, and operation identifiers."""

from .catalog import AUTHN_OPERATIONS, AuthnOperationEntry, AuthnPreset
from .facades import AuthnFacade
from .factories import build_authn_registry
from .operations import AuthnKernelOp

# ----------------------- #

__all__ = [
    "AUTHN_OPERATIONS",
    "AuthnKernelOp",
    "AuthnFacade",
    "AuthnOperationEntry",
    "AuthnPreset",
    "build_authn_registry",
]
