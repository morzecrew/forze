"""Tenant context port for multi-tenant applications.

Provides :class:`TenantContextPort` for ambient tenant identity. Used when
routing storage, document, or other resources to tenant-specific backends.
"""

from typing import Protocol, runtime_checkable
from uuid import UUID

# ----------------------- #


@runtime_checkable
class TenantContextPort(Protocol):
    """Access to the current tenant identifier in the ambient context.

    Implementations typically use context variables or request-scoped storage.
    Used for tenant isolation in multi-tenant document and storage backends.
    """

    def get(self) -> UUID:
        """Return the current tenant identifier.

        Implementations should raise if no tenant is currently bound.
        """
        ...

    def set(self, tenant_id: UUID) -> None:
        """Bind the current tenant identifier for the ambient context."""
        ...
