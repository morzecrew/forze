"""Configurable authz dependency factories resolving document ports from execution context."""

from typing import final

import attrs

from forze.application.contracts.authz import (
    AuthzDecisionPort,
    AuthzScopePort,
    AuthzSpec,
    DelegationGrantPort,
    DelegationPort,
    GrantQueryPort,
    PrincipalRegistryPort,
    RoleAssignmentPort,
)
from forze.application.execution import ExecutionContext

from ...adapters import (
    AuthzDecisionAdapter,
    AuthzScopeAdapter,
    DelegationGrantAdapter,
    DelegationQueryAdapter,
    GrantQueryAdapter,
    PrincipalRegistryAdapter,
    RoleAssignmentAdapter,
)
from ...application.specs import (
    delegation_grant_spec,
    group_permission_binding_spec,
    group_principal_binding_spec,
    group_role_binding_spec,
    group_spec,
    permission_definition_spec,
    policy_principal_spec,
    principal_permission_binding_spec,
    principal_role_binding_spec,
    role_definition_spec,
    role_permission_binding_spec,
)
from ...services.grants import AuthzGrantResolver, AuthzGrantResolverDeps
from .configs import AuthzSharedServices


# ----------------------- #


def _grant_resolver(ctx: ExecutionContext) -> AuthzGrantResolver:
    # The invocation tenant is what the (tenant-aware) document ports auto-scope to;
    # pass it so the resolver can refuse a scope that names a different tenant.
    tenant = ctx.inv_ctx.get_tenant()

    return AuthzGrantResolver(
        deps=AuthzGrantResolverDeps(
            permission_qry=ctx.doc.query(permission_definition_spec),
            role_qry=ctx.doc.query(role_definition_spec),
            group_qry=ctx.doc.query(group_spec),
            rp_binding_qry=ctx.doc.query(role_permission_binding_spec),
            pr_binding_qry=ctx.doc.query(principal_role_binding_spec),
            pp_binding_qry=ctx.doc.query(principal_permission_binding_spec),
            gp_binding_qry=ctx.doc.query(group_principal_binding_spec),
            gr_binding_qry=ctx.doc.query(group_role_binding_spec),
            gperm_binding_qry=ctx.doc.query(group_permission_binding_spec),
        ),
        invocation_tenant_id=tenant.tenant_id if tenant is not None else None,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePrincipalRegistry:
    """Build :class:`~forze_authz.adapters.principal_registry.PrincipalRegistryAdapter`."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthzSpec,
    ) -> PrincipalRegistryPort:
        return PrincipalRegistryAdapter(
            principal_qry=ctx.doc.query(policy_principal_spec),
            principal_cmd=ctx.doc.command(policy_principal_spec),
        )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRoleAssignment:
    """Build :class:`~forze_authz.adapters.role_assignment.RoleAssignmentAdapter`."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthzSpec,
    ) -> RoleAssignmentPort:
        return RoleAssignmentAdapter(
            spec=spec,
            principal_qry=ctx.doc.query(policy_principal_spec),
            role_qry=ctx.doc.query(role_definition_spec),
            pr_binding_cmd=ctx.doc.command(principal_role_binding_spec),
            pr_binding_qry=ctx.doc.query(principal_role_binding_spec),
            resolver=_grant_resolver(ctx),
        )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableGrantQuery:
    """Build :class:`~forze_authz.adapters.effective_grants.GrantQueryAdapter`."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthzSpec,
    ) -> GrantQueryPort:
        return GrantQueryAdapter(
            spec=spec,
            principal_qry=ctx.doc.query(policy_principal_spec),
            resolver=_grant_resolver(ctx),
        )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableDelegationQuery:
    """Build :class:`~forze_authz.adapters.delegation.DelegationQueryAdapter`."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthzSpec,
    ) -> DelegationPort:
        return DelegationQueryAdapter(
            spec=spec,
            grant_qry=ctx.doc.query(delegation_grant_spec),
        )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableDelegationGrant:
    """Build :class:`~forze_authz.adapters.delegation.DelegationGrantAdapter`."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthzSpec,
    ) -> DelegationGrantPort:
        return DelegationGrantAdapter(
            spec=spec,
            principal_qry=ctx.doc.query(policy_principal_spec),
            grant_qry=ctx.doc.query(delegation_grant_spec),
            grant_cmd=ctx.doc.command(delegation_grant_spec),
        )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableAuthzDecision:
    """Build :class:`~forze_authz.adapters.authorization.AuthzDecisionAdapter`."""

    shared: AuthzSharedServices

    def __call__(self, ctx: ExecutionContext, spec: AuthzSpec) -> AuthzDecisionPort:
        return AuthzDecisionAdapter(
            spec=spec,
            principal_qry=ctx.doc.query(policy_principal_spec),
            resolver=_grant_resolver(ctx),
            policy=self.shared.policy,
        )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableAuthzScope:
    """Build :class:`~forze_authz.adapters.scoping.AuthzScopeAdapter`."""

    shared: AuthzSharedServices

    def __call__(self, ctx: ExecutionContext, spec: AuthzSpec) -> AuthzScopePort:
        return AuthzScopeAdapter(
            spec=spec,
            principal_qry=ctx.doc.query(policy_principal_spec),
            resolver=_grant_resolver(ctx),
            policy=self.shared.policy,
        )
