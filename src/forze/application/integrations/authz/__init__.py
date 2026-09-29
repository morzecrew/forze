"""Shared authz integration helpers over the authz contracts."""

from .providers import DEFAULT_PROVIDER_TIMEOUT, derive_permissions

# ----------------------- #

__all__ = [
    "DEFAULT_PROVIDER_TIMEOUT",
    "derive_permissions",
]
