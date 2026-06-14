"""Tenant infrastructure provisioning — create/tear down per-tenant resources on onboarding.

The ``namespace`` and ``dedicated`` isolation tiers (see :mod:`.wiring`) assume the per-tenant
namespace already exists — an object-store bucket, a DB schema, a warehouse dataset. A
:class:`TenantProvisionerPort` is the seam that creates those resources when a tenant is
onboarded (and tears them down when offboarded), so the higher tiers are operationally real
rather than assuming hand-provisioned infrastructure.

Provisioners are **idempotent** (provisioning is retried after partial failure, and a tenant
may be re-provisioned) and receive the :class:`TenantIdentity` explicitly — the tenant being
provisioned is generally *not* the ambient bound tenant (an admin onboards tenant X while not
acting as X), so a provisioner must scope by the passed identity, never the context.
"""

from typing import Awaitable, Callable, Protocol, Sequence, runtime_checkable

import attrs

from .value_objects import TenantIdentity

# ----------------------- #


@runtime_checkable
class TenantProvisionerPort(Protocol):
    """Create / tear down a tenant's per-tenant infrastructure (idempotent)."""

    def provision(self, tenant: TenantIdentity) -> Awaitable[None]:
        """Ensure *tenant*'s resources exist (create-if-missing; safe to retry)."""
        ...  # pragma: no cover

    def deprovision(self, tenant: TenantIdentity) -> Awaitable[None]:
        """Tear down *tenant*'s resources (safe to call when already absent)."""
        ...  # pragma: no cover


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class NoopTenantProvisioner(TenantProvisionerPort):
    """Provisioner that does nothing — the default when no infrastructure needs creating."""

    async def provision(self, tenant: TenantIdentity) -> None:
        return None

    async def deprovision(self, tenant: TenantIdentity) -> None:
        return None


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class FunctionTenantProvisioner(TenantProvisionerPort):
    """Adapt provision / deprovision callables into a :class:`TenantProvisionerPort`.

    ``on_deprovision`` defaults to a no-op — destructive teardown is opt-in, since deleting a
    tenant's data (a bucket, a schema) is rarely something to do implicitly.
    """

    on_provision: Callable[[TenantIdentity], Awaitable[None]]
    on_deprovision: Callable[[TenantIdentity], Awaitable[None]] | None = None

    # ....................... #

    async def provision(self, tenant: TenantIdentity) -> None:
        await self.on_provision(tenant)

    # ....................... #

    async def deprovision(self, tenant: TenantIdentity) -> None:
        if self.on_deprovision is not None:
            await self.on_deprovision(tenant)


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class CompositeTenantProvisioner(TenantProvisionerPort):
    """Run a sequence of provisioners — one per integration that needs per-tenant resources.

    Provisioning runs in declared order; tear-down runs in **reverse** order (so a resource
    created last is removed first). Each step's idempotency lets a failed onboarding be safely
    retried.
    """

    provisioners: Sequence[TenantProvisionerPort] = ()

    # ....................... #

    async def provision(self, tenant: TenantIdentity) -> None:
        for provisioner in self.provisioners:
            await provisioner.provision(tenant)

    # ....................... #

    async def deprovision(self, tenant: TenantIdentity) -> None:
        for provisioner in reversed(list(self.provisioners)):
            await provisioner.deprovision(tenant)
