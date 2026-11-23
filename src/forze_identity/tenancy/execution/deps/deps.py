"""Configurable factories wiring document ports from execution context."""

from typing import final

import attrs

from forze.application.contracts.authz import AuthzSpec, RoleAssignmentDepKey
from forze.application.contracts.deps import Requirement
from forze.application.contracts.tenancy import (
    TenantManagementDepPort,
    TenantManagementPort,
    TenantProvisionerPort,
    TenantResolverDepPort,
    TenantResolverPort,
)
from forze.application.execution import ExecutionContext
from forze.base.primitives import StrKey

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
        tenant_qry = ctx.document.query(tenant_spec) if self.verify_tenant_active else None

        return TenantResolverAdapter(
            binding_qry=ctx.document.query(principal_tenant_binding_spec),
            tenant_qry=tenant_qry,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableTenantManagement(TenantManagementDepPort):
    """Build :class:`~forze_tenancy.adapters.management.TenantManagementAdapter`."""

    provisioner: TenantProvisionerPort | None = None
    """Optional per-tenant infrastructure provisioner run on ``provision_tenant``."""

    authz_route: StrKey | None = None
    """Authz route whose role assignment ``detach_principal`` revokes the tenant's roles
    through. ``None`` keeps them."""

    # ....................... #

    def requirements(self) -> tuple[Requirement, ...]:
        """The role assignment on :attr:`authz_route`, so a route that is not wired is refused
        when the registry is frozen instead of on the first detach."""

        if self.authz_route is None:
            return ()

        return (
            Requirement(
                key=RoleAssignmentDepKey,
                route=self.authz_route,
                remedy=(
                    "Wire the authz plane's role assignment on that route "
                    "(AuthzDepsModule(role_assignment=...)), or name the route it is wired on "
                    "in TenancyDepsModule(authz_route=...)."
                ),
            ),
        )

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> TenantManagementPort:
        role_assignment = (
            ctx.authz.role_assignment(AuthzSpec(name=self.authz_route))
            if self.authz_route is not None
            else None
        )

        return TenantManagementAdapter(
            tenant_qry=ctx.document.query(tenant_spec),
            tenant_cmd=ctx.document.command(tenant_spec),
            binding_qry=ctx.document.query(principal_tenant_binding_spec),
            binding_cmd=ctx.document.command(principal_tenant_binding_spec),
            provisioner=self.provisioner,
            role_assignment=role_assignment,
        )
