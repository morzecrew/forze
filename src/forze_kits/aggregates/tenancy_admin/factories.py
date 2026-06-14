"""Factory for the tenancy-admin registry (create tenant, manage members)."""

from forze.application.execution import ExecutionContext
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKeyNamespace

from .dto import (
    CreatedTenantDTO,
    CreateTenantRequestDTO,
    MemberListDTO,
    MembershipDTO,
    TenantRefDTO,
)
from .handlers import (
    CreateTenant,
    DeactivateTenant,
    InviteMember,
    ListMembers,
    RemoveMember,
)
from .operations import TenancyAdminKernelOp

# ----------------------- #


def build_tenancy_admin_registry(ns: StrKeyNamespace) -> OperationRegistry:
    """Build the tenancy-admin registry (create / list-members / invite / remove / deactivate).

    These operations manage *arbitrary* tenants and memberships — the privileged inverse of the
    self-service selector — so the framework cannot guard them generically: *who* may create a
    tenant, invite a member, or deactivate a tenant is the app's authorization model, which Forze
    does not define. They therefore ship **unguarded** (no ``AuthnRequired``, no authz). Bind
    your guards on every operation before exposing them — the same chain as ``deactivate_principal``::

        reg = build_tenancy_admin_registry(ns)
        AUTHZ = AuthzSpec(name="api")
        for op in TenancyAdminKernelOp:
            reg = (
                reg.bind(ns.key(op))
                .bind_outer()
                .before(
                    AuthnRequired().to_step(),
                    AuthzBeforeAuthorize(spec=AUTHZ, action=f"tenants:{op}").to_step(),
                )
                .finish(deep=True)
            )
        registry = reg.freeze()

    Returned **unfrozen** so the app can layer those hooks; ``list_members`` is pre-classified as
    a query (a read). Project the result with
    :func:`~forze_fastapi.attach_tenancy_admin_routes`. Requires a ``TenancyDepsModule`` wiring a
    ``tenant_management`` route.
    """

    def _create_tenant(ctx: ExecutionContext) -> CreateTenant:
        return CreateTenant(tenant_management=ctx.tenancy.require_manager())

    def _list_members(ctx: ExecutionContext) -> ListMembers:
        return ListMembers(tenant_management=ctx.tenancy.require_manager())

    def _invite_member(ctx: ExecutionContext) -> InviteMember:
        return InviteMember(tenant_management=ctx.tenancy.require_manager())

    def _remove_member(ctx: ExecutionContext) -> RemoveMember:
        return RemoveMember(tenant_management=ctx.tenancy.require_manager())

    def _deactivate_tenant(ctx: ExecutionContext) -> DeactivateTenant:
        return DeactivateTenant(tenant_management=ctx.tenancy.require_manager())

    reg = OperationRegistry(
        handlers={
            ns.key(TenancyAdminKernelOp.CREATE_TENANT): _create_tenant,
            ns.key(TenancyAdminKernelOp.LIST_MEMBERS): _list_members,
            ns.key(TenancyAdminKernelOp.INVITE_MEMBER): _invite_member,
            ns.key(TenancyAdminKernelOp.REMOVE_MEMBER): _remove_member,
            ns.key(TenancyAdminKernelOp.DEACTIVATE_TENANT): _deactivate_tenant,
        },
    )

    reg = reg.set_descriptors(
        {
            TenancyAdminKernelOp.CREATE_TENANT: OperationDescriptor(
                input_type=CreateTenantRequestDTO,
                output_type=CreatedTenantDTO,
                description="Provision a new tenant and return its identity.",
            ),
            TenancyAdminKernelOp.LIST_MEMBERS: OperationDescriptor(
                input_type=TenantRefDTO,
                output_type=MemberListDTO,
                description="List the principal ids that belong to a tenant.",
            ),
            TenancyAdminKernelOp.INVITE_MEMBER: OperationDescriptor(
                input_type=MembershipDTO,
                description="Grant a principal membership in a tenant (idempotent).",
            ),
            TenancyAdminKernelOp.REMOVE_MEMBER: OperationDescriptor(
                input_type=MembershipDTO,
                description="Revoke a principal's membership in a tenant.",
            ),
            TenancyAdminKernelOp.DEACTIVATE_TENANT: OperationDescriptor(
                input_type=TenantRefDTO,
                description="Disable a tenant (record lifecycle; not infra teardown).",
            ),
        },
        namespace=ns,
    )

    # ``list_members`` is a read — classify it QUERY. Every op stays **unguarded** here by
    # design: admin authorization is app-specific, so the app binds ``AuthnRequired`` +
    # ``AuthzBeforeAuthorize`` on each before exposing them (see the docstring).
    return reg.bind(ns.key(TenancyAdminKernelOp.LIST_MEMBERS)).as_query().finish()
