"""Configurable authz dependency factories resolving document ports from execution context."""

from typing import final

import attrs

from forze.application.contracts.authz import (
    AuthzPort,
    AuthzSpec,
    EffectiveGrantsPort,
    PrincipalRegistryPort,
    RoleAssignmentPort,
)
from forze.application.execution import ExecutionContext

from ...adapters import (
    AuthzAdapter,
    EffectiveGrantsAdapter,
    PrincipalRegistryAdapter,
    RoleAssignmentAdapter,
)
from ...application.specs import (
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
    return AuthzGrantResolver(
        deps=AuthzGrantResolverDeps(
            permission_qry=ctx.doc_query(permission_definition_spec),
            role_qry=ctx.doc_query(role_definition_spec),
            group_qry=ctx.doc_query(group_spec),
            rp_binding_qry=ctx.doc_query(role_permission_binding_spec),
            pr_binding_qry=ctx.doc_query(principal_role_binding_spec),
            pp_binding_qry=ctx.doc_query(principal_permission_binding_spec),
            gp_binding_qry=ctx.doc_query(group_principal_binding_spec),
            gr_binding_qry=ctx.doc_query(group_role_binding_spec),
            gperm_binding_qry=ctx.doc_query(group_permission_binding_spec),
        ),
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurablePrincipalRegistry:
    """Build :class:`~forze_authz.adapters.principal_registry.PrincipalRegistryAdapter`."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthzSpec,
    ) -> PrincipalRegistryPort:
        _ = spec

        return PrincipalRegistryAdapter(
            principal_qry=ctx.doc_query(policy_principal_spec),
            principal_cmd=ctx.doc_command(policy_principal_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableRoleAssignment:
    """Build :class:`~forze_authz.adapters.role_assignment.RoleAssignmentAdapter`."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthzSpec,
    ) -> RoleAssignmentPort:
        _ = spec

        return RoleAssignmentAdapter(
            principal_qry=ctx.doc_query(policy_principal_spec),
            role_qry=ctx.doc_query(role_definition_spec),
            pr_binding_qry=ctx.doc_query(principal_role_binding_spec),
            pr_binding_cmd=ctx.doc_command(principal_role_binding_spec),
            resolver=_grant_resolver(ctx),
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableEffectiveGrants:
    """Build :class:`~forze_authz.adapters.effective_grants.EffectiveGrantsAdapter`."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthzSpec,
    ) -> EffectiveGrantsPort:
        _ = spec

        return EffectiveGrantsAdapter(
            principal_qry=ctx.doc_query(policy_principal_spec),
            resolver=_grant_resolver(ctx),
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableAuthz:
    """Build :class:`~forze_authz.adapters.authorization.AuthzAdapter`."""

    shared: AuthzSharedServices

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthzSpec) -> AuthzPort:
        _ = spec

        return AuthzAdapter(
            principal_qry=ctx.doc_query(policy_principal_spec),
            resolver=_grant_resolver(ctx),
            policy=self.shared.policy,
        )
