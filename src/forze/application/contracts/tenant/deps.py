"""Tenant dependency keys."""

from typing import Protocol, runtime_checkable

from ..deps import DepKey
from .ports import TenantContextPort

# ----------------------- #


@runtime_checkable
class TenantContextDepPort(Protocol):
    """Factory protocol for building :class:`TenantContextPort` instances."""

    def __call__(self) -> TenantContextPort:
        """Build a tenant context port."""
        ...


# ....................... #

TenantContextDepKey = DepKey[TenantContextDepPort]("tenant_context")
"""Key used to register the :class:`TenantContextDepPort` implementation."""
