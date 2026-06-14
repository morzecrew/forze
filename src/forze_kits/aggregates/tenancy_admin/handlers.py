"""Admin handlers for tenant and membership management.

Unlike the self-service selector, these act on *arbitrary* tenants/principals taken from the
request (not the bound identity). They carry no authn/authz guard of their own — the registry
ships them unguarded and the app binds ``AuthnRequired`` + an ``AuthzBeforeAuthorize`` before
exposing them (see :func:`.factories.build_tenancy_admin_registry`).
"""

import attrs

from forze.application.contracts.execution import Handler
from forze.application.contracts.tenancy import TenantManagementPort

from .dto import (
    CreatedTenantDTO,
    CreateTenantRequestDTO,
    MemberListDTO,
    MemberListItemDTO,
    MembershipDTO,
    TenantRefDTO,
)

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreateTenant(Handler[CreateTenantRequestDTO, CreatedTenantDTO]):
    """Provision a new tenant."""

    tenant_management: TenantManagementPort

    async def __call__(self, args: CreateTenantRequestDTO) -> CreatedTenantDTO:
        identity = await self.tenant_management.provision_tenant(
            tenant_key=args.tenant_key
        )

        return CreatedTenantDTO(
            tenant_id=identity.tenant_id,
            tenant_key=identity.tenant_key,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ListMembers(Handler[TenantRefDTO, MemberListDTO]):
    """List the principals that belong to a tenant."""

    tenant_management: TenantManagementPort

    async def __call__(self, args: TenantRefDTO) -> MemberListDTO:
        principal_ids = await self.tenant_management.list_tenant_principals(args.id)

        return MemberListDTO(
            members=[
                MemberListItemDTO(principal_id=principal_id)
                for principal_id in principal_ids
            ]
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class InviteMember(Handler[MembershipDTO, None]):
    """Grant a principal membership in a tenant (idempotent)."""

    tenant_management: TenantManagementPort

    async def __call__(self, args: MembershipDTO) -> None:
        await self.tenant_management.attach_principal(args.principal_id, args.tenant_id)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RemoveMember(Handler[MembershipDTO, None]):
    """Revoke a principal's membership in a tenant."""

    tenant_management: TenantManagementPort

    async def __call__(self, args: MembershipDTO) -> None:
        await self.tenant_management.detach_principal(args.principal_id, args.tenant_id)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeactivateTenant(Handler[TenantRefDTO, None]):
    """Disable a tenant (record lifecycle; infra teardown is ``deprovision_tenant``)."""

    tenant_management: TenantManagementPort

    async def __call__(self, args: TenantRefDTO) -> None:
        await self.tenant_management.deactivate_tenant(args.id)
