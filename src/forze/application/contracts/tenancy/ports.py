from collections.abc import Awaitable, Sequence
from typing import Protocol
from uuid import UUID

from .value_objects import TenantIdentity

# ----------------------- #


class TenantResolverPort(Protocol):
    """Port for resolving the tenant identity."""

    def resolve_from_principal(
        self,
        principal_id: UUID,
        *,
        requested_tenant_id: UUID | None = None,
    ) -> Awaitable[TenantIdentity | None]:
        """Resolve the tenant identity from the principal ID.

        ``requested_tenant_id`` is an optional non-authoritative tenant request
        (for example from a verified issuer claim or HTTP header). Implementations
        should validate it against principal membership rather than trusting it.
        """
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

    def list_principal_tenants(
        self,
        principal_id: UUID,
    ) -> Awaitable[Sequence[TenantIdentity]]:
        """List the active tenants a principal belongs to (the basis of a tenant selector).

        Returns one :class:`TenantIdentity` (id + key) per active membership; inactive
        tenants are omitted. Membership-scoped, so it is safe to expose to the principal as a
        "switch organization" list.
        """
        ...

    def list_tenant_principals(
        self,
        tenant_id: UUID,
    ) -> Awaitable[Sequence[UUID]]:
        """List the principal ids that are members of *tenant_id* (the admin inverse of
        :meth:`list_principal_tenants`).

        Returns principal ids only; joining them with principal details (login, name) is the
        caller's concern (those live in the identity plane). Expose this only on an
        authorization-gated admin surface.
        """
        ...

    def list_tenants(
        self,
        limit: int = 100,
        offset: int = 0,
        *,
        active_only: bool = False,
    ) -> Awaitable[tuple[Sequence[TenantIdentity], int]]:
        """Page through **every** tenant, with the total. Not membership-scoped.

        The global enumeration that drives per-tenant work: a sweep, a migration, an export.
        :meth:`list_principal_tenants` cannot do that job — it answers "which tenants may
        *this* principal see", so anything driven from it visits only the tenants somebody
        happens to be a member of.

        ``active_only=False`` is the default **on purpose**. Deactivating a tenant sets a
        flag; it does not delete a row, so a deactivated tenant's documents, blobs and
        counters are all still there. A sweep that quietly skipped them would drop real data
        and report success — so the complete answer is what you get unless you ask for less.
        Pass ``active_only=True`` for a "who is live" admin view, where that is the question.

        Expose this only on an authorization-gated admin surface: it lists every tenant in
        the deployment, including ones the caller has no membership in.
        """
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

    def deprovision_tenant(self, tenant_id: UUID) -> Awaitable[None]:
        """Tear down a tenant's per-tenant infrastructure (the inverse of provisioning).

        Runs the configured ``TenantProvisionerPort``'s ``deprovision`` for the tenant. It
        is the infrastructure counterpart of :meth:`provision_tenant`; the *record*
        lifecycle (e.g. :meth:`deactivate_tenant`) is separate, so a full offboarding calls
        both. No-op when no provisioner is wired.
        """
        ...


# ....................... #


class TenantProviderPort(Protocol):
    """Port for providing the tenant ID."""

    def __call__(self) -> TenantIdentity | None:
        """Provide the tenant ID."""
        ...
