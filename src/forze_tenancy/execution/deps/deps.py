"""Configurable factories wiring document ports from execution context."""

from typing import final

import attrs

from forze.application.contracts.tenancy import (
    TenantManagementDepPort,
    TenantManagementPort,
    TenantResolverDepPort,
    TenantResolverPort,
)
from forze.application.execution import ExecutionContext

from ...adapters import TenantManagementAdapter, TenantResolverAdapter
from ...application.specs import principal_tenant_binding_spec, tenant_spec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableTenantResolver(TenantResolverDepPort):
    """Build :class:`~forze_tenancy.adapters.resolver.TenantResolverAdapter`."""

    verify_tenant_active: bool = True
    """When ``True``, resolve :data:`~forze_tenancy.application.specs.tenant_spec` and drop inactive tenants."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> TenantResolverPort:
        tenant_qry = ctx.doc_query(tenant_spec) if self.verify_tenant_active else None

        return TenantResolverAdapter(
            binding_qry=ctx.doc_query(principal_tenant_binding_spec),
            tenant_qry=tenant_qry,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableTenantManagement(TenantManagementDepPort):
    """Build :class:`~forze_tenancy.adapters.management.TenantManagementAdapter`."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> TenantManagementPort:
        return TenantManagementAdapter(
            tenant_qry=ctx.doc_query(tenant_spec),
            tenant_cmd=ctx.doc_command(tenant_spec),
            binding_qry=ctx.doc_query(principal_tenant_binding_spec),
            binding_cmd=ctx.doc_command(principal_tenant_binding_spec),
        )
