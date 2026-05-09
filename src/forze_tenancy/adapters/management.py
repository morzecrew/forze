from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.contracts.tenancy import TenantIdentity, TenantManagementPort
from forze.base.errors import CoreError

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

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.tenant_qry.spec.name != tenant_spec.name:
            raise CoreError("tenant_qry spec must match tenant_spec")

        if self.tenant_cmd.spec.name != tenant_spec.name:
            raise CoreError("tenant_cmd spec must match tenant_spec")

        if self.binding_qry.spec.name != principal_tenant_binding_spec.name:
            raise CoreError("binding_qry spec must match principal_tenant_binding_spec")

        if self.binding_cmd.spec.name != principal_tenant_binding_spec.name:
            raise CoreError("binding_cmd spec must match principal_tenant_binding_spec")

    # ....................... #

    async def provision_tenant(
        self,
        *,
        tenant_key: str | None = None,
    ) -> TenantIdentity:
        row = await self.tenant_cmd.create(CreateTenantCmd(tenant_key=tenant_key))

        return TenantIdentity(tenant_id=row.id, tenant_key=row.tenant_key)

    # ....................... #

    async def attach_principal(self, principal_id: UUID, tenant_id: UUID) -> None:
        dup = await self.binding_qry.find_many(
            filters={
                "$fields": {
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
                "$fields": {
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
