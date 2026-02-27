"""Tenant context port for multi-tenant applications."""

from typing import Protocol, runtime_checkable
from uuid import UUID

# ----------------------- #


@runtime_checkable
class TenantContextPort(Protocol):
    """Access to the current tenant identifier."""

    def get(self) -> UUID:
        """Return the current tenant identifier.

        Implementations should raise if no tenant is currently bound.
        """
        ...

    def set(self, tenant_id: UUID) -> None:
        """Bind the current tenant identifier for the ambient context."""
        ...
