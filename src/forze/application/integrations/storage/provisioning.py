"""Object-storage tenant provisioner — ensure a tenant's bucket exists on onboarding.

The reference :class:`~forze.application.contracts.tenancy.TenantProvisionerPort` for object
storage: on ``provision`` it resolves the tenant's bucket (a per-tenant resolver for the
``schema`` tier, or the shared static bucket for the ``row`` tier) and ensures it exists.
"""

import attrs

from forze.application.contracts.resolution import NamedResourceSpec, resolve_value
from forze.application.contracts.tenancy import TenantIdentity, TenantProvisionerPort
from forze.application.integrations.storage.client import ObjectStorageClientPort

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class ObjectStorageTenantProvisioner(TenantProvisionerPort):
    """Ensure a tenant's object-storage bucket exists when the tenant is onboarded.

    Pair this with the bucket spec used by the storage adapter: a per-tenant ``bucket``
    resolver (``lambda t: f"tenant-{t}"``) provisions a bucket per tenant (``schema`` tier);
    a static name ensures the single shared bucket (idempotent, harmless under the ``row``
    tier). Teardown is a deliberate no-op — buckets are not auto-deleted (the client exposes
    no delete, and destroying a tenant's data implicitly is unsafe); remove them out-of-band.
    """

    client: ObjectStorageClientPort
    bucket: NamedResourceSpec

    async def provision(self, tenant: TenantIdentity) -> None:
        bucket = await resolve_value(self.bucket, tenant.tenant_id)
        await self.client.ensure_bucket(bucket)

    async def deprovision(self, tenant: TenantIdentity) -> None:
        return None
