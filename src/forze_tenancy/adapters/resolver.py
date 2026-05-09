from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentQueryPort
from forze.application.contracts.tenancy import TenantIdentity, TenantResolverPort
from forze.base.errors import CoreError

from ..application.specs import principal_tenant_binding_spec, tenant_spec
from ..domain.models.principal_tenant_binding import ReadPrincipalTenantBinding
from ..domain.models.tenant import ReadTenant

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TenantResolverAdapter(TenantResolverPort):
    """Resolve tenant from principal via principal–tenant binding documents."""

    binding_qry: DocumentQueryPort[ReadPrincipalTenantBinding]
    """Query port for :data:`~forze_tenancy.application.specs.principal_tenant_binding_spec`."""

    tenant_qry: DocumentQueryPort[ReadTenant] | None = None
    """When set, membership is ignored if tenant is inactive."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.binding_qry.spec.name != principal_tenant_binding_spec.name:
            raise CoreError("binding_qry spec must match principal_tenant_binding_spec")

        if (
            self.tenant_qry is not None
            and self.tenant_qry.spec.name != tenant_spec.name
        ):
            raise CoreError("tenant_qry spec must match tenant_spec")

    # ....................... #

    async def resolve_from_principal(
        self,
        principal_id: UUID,
    ) -> TenantIdentity | None:
        page = await self.binding_qry.find_many(
            filters={"$fields": {"principal_id": principal_id}},
            pagination={"limit": 1},
        )

        if not page.hits:
            return None

        bind = page.hits[0]
        tid = bind.tenant_id

        if self.tenant_qry is None:
            return TenantIdentity(tenant_id=tid)

        tenant = await self.tenant_qry.get(tid)

        if not tenant.is_active:
            return None

        return TenantIdentity(tenant_id=tid, tenant_key=tenant.tenant_key)
