from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.contracts.tenancy import (
    TenantIdentity,
    TenantManagementPort,
    TenantProvisionerPort,
)
from forze.base.exceptions import exc
from forze_identity._secure_spec import forbid_cache_and_history

from ..application.specs import principal_tenant_binding_spec, tenant_spec
from ..domain.models.principal_tenant_binding import (
    CreatePrincipalTenantBindingCmd,
    PrincipalTenantBinding,
    ReadPrincipalTenantBinding,
)
from ..domain.models.tenant import (
    CreateTenantCmd,
    ReadTenant,
    Tenant,
    UpdateTenantCmd,
)

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TenantManagementAdapter(TenantManagementPort):
    """Document-backed :class:`~forze.application.contracts.tenancy.TenantManagementPort`."""

    tenant_qry: DocumentQueryPort[ReadTenant]
    tenant_cmd: DocumentCommandPort[
        ReadTenant, Tenant, CreateTenantCmd, UpdateTenantCmd
    ]
    binding_qry: DocumentQueryPort[ReadPrincipalTenantBinding]
    binding_cmd: DocumentCommandPort[
        ReadPrincipalTenantBinding,
        PrincipalTenantBinding,
        CreatePrincipalTenantBindingCmd,
        Any,
    ]
    provisioner: TenantProvisionerPort | None = None
    """Optional per-tenant infrastructure provisioner run on :meth:`provision_tenant`.

    When set, onboarding creates the tenant record then ensures its resources (bucket /
    schema / dataset) exist — so the ``namespace``/``dedicated`` isolation tiers don't assume
    hand-provisioned infrastructure. ``None`` (the default) leaves onboarding record-only.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.tenant_qry.spec.name != tenant_spec.name:
            raise exc.internal("tenant_qry spec must match tenant_spec")

        if self.tenant_cmd.spec.name != tenant_spec.name:
            raise exc.internal("tenant_cmd spec must match tenant_spec")

        if self.binding_qry.spec.name != principal_tenant_binding_spec.name:
            raise exc.internal(
                "binding_qry spec must match principal_tenant_binding_spec"
            )

        if self.binding_cmd.spec.name != principal_tenant_binding_spec.name:
            raise exc.internal(
                "binding_cmd spec must match principal_tenant_binding_spec"
            )

        forbid_cache_and_history(
            self.tenant_qry.spec,
            self.tenant_cmd.spec,
            label="Tenant",
        )
        forbid_cache_and_history(
            self.binding_qry.spec,
            self.binding_cmd.spec,
            label="Principal-tenant binding",
        )

    # ....................... #

    async def provision_tenant(
        self,
        *,
        tenant_key: str | None = None,
    ) -> TenantIdentity:
        row = await self.tenant_cmd.create(CreateTenantCmd(tenant_key=tenant_key))
        identity = TenantIdentity(tenant_id=row.id, tenant_key=row.tenant_key)

        # Record first, then infrastructure: a provisioner failure leaves the record so the
        # idempotent provisioning can be retried.
        if self.provisioner is not None:
            await self.provisioner.provision(identity)

        return identity

    # ....................... #

    async def attach_principal(self, principal_id: UUID, tenant_id: UUID) -> None:
        dup = await self.binding_qry.find_many(
            filters={
                "$values": {
                    "principal_id": principal_id,
                    "tenant_id": tenant_id,
                },
            },
            pagination={"limit": 1},
        )

        if dup.hits:
            return

        await self.binding_cmd.create(
            CreatePrincipalTenantBindingCmd(
                principal_id=principal_id,
                tenant_id=tenant_id,
            ),
            return_new=False,
        )

    # ....................... #

    async def detach_principal(self, principal_id: UUID, tenant_id: UUID) -> None:
        page = await self.binding_qry.find_many(
            filters={
                "$values": {
                    "principal_id": principal_id,
                    "tenant_id": tenant_id,
                },
            },
        )

        for hit in page.hits:
            await self.binding_cmd.kill(hit.id)

    # ....................... #

    async def deactivate_tenant(self, tenant_id: UUID) -> None:
        row = await self.tenant_qry.get(tenant_id)

        await self.tenant_cmd.update(
            tenant_id,
            row.rev,
            UpdateTenantCmd(is_active=False),
            return_new=False,
        )

    # ....................... #

    async def deprovision_tenant(self, tenant_id: UUID) -> None:
        if self.provisioner is None:
            return

        row = await self.tenant_qry.get(tenant_id)

        await self.provisioner.deprovision(
            TenantIdentity(tenant_id=row.id, tenant_key=row.tenant_key)
        )
