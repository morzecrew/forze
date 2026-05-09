from typing import Awaitable, Protocol
from uuid import UUID

from .value_objects import TenantIdentity

# ----------------------- #


class TenantResolverPort(Protocol):
    """Port for resolving the tenant identity."""

    def resolve_from_principal(
        self,
        principal_id: UUID,
    ) -> Awaitable[TenantIdentity | None]:
        """Resolve the tenant identity from the principal ID."""
        ...


# ....................... #


class TenantManagementPort(Protocol):
    """Lifecycle operations on tenants and principal membership (not CRUD-generic)."""

    def provision_tenant(
        self,
        *,
        tenant_key: str | None = None,
    ) -> Awaitable[TenantIdentity]:
        """Create a tenant aggregate and return its identity."""
        ...

    def attach_principal(
        self,
        principal_id: UUID,
        tenant_id: UUID,
    ) -> Awaitable[None]:
        """Grant membership (idempotent if binding already exists)."""
        ...

    def detach_principal(
        self,
        principal_id: UUID,
        tenant_id: UUID,
    ) -> Awaitable[None]:
        """Revoke membership."""
        ...

    def deactivate_tenant(self, tenant_id: UUID) -> Awaitable[None]:
        """Disable tenant (exact semantics adapter-defined, e.g. soft deactivate)."""
        ...


# ....................... #


class TenantProviderPort(Protocol):
    """Port for providing the tenant ID."""

    def __call__(self) -> TenantIdentity | None:
        """Provide the tenant ID."""
        ...
