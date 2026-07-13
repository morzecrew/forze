"""Authz dependency module for the application kernel."""

from collections.abc import Collection
from typing import final

import attrs

from forze.application.contracts.authz import (
    AuthzDecisionDepKey,
    AuthzScopeDepKey,
    DelegationDepKey,
    DelegationGrantDepKey,
    GrantQueryDepKey,
    PrincipalRegistryDepKey,
    RoleAssignmentDepKey,
)
from forze.application.contracts.deps import Deps, DepsModule
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze_identity._routes import normalize_route_set as _normalize_route_set

from .configs import AuthzKernelConfig, build_authz_shared_services
from .deps import (
    ConfigurableAuthzDecision,
    ConfigurableAuthzScope,
    ConfigurableDelegationGrant,
    ConfigurableDelegationQuery,
    ConfigurableGrantQuery,
    ConfigurablePrincipalRegistry,
    ConfigurableRoleAssignment,
)

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzDepsModule(DepsModule):
    """Registers authz dependency factories that resolve document ports via execution context."""

    kernel: AuthzKernelConfig | None = attrs.field(default=None)
    principal_registry: Collection[StrKey] | None = attrs.field(default=None)
    role_assignment: Collection[StrKey] | None = attrs.field(default=None)
    grant_query: Collection[StrKey] | None = attrs.field(default=None)
    delegation: Collection[StrKey] | None = attrs.field(default=None)
    delegation_grant: Collection[StrKey] | None = attrs.field(default=None)
    decision: Collection[StrKey] | None = attrs.field(default=None)
    scope: Collection[StrKey] | None = attrs.field(default=None)

    def __call__(self) -> Deps:
        pr = _normalize_route_set(self.principal_registry)
        ra = _normalize_route_set(self.role_assignment)
        gq = _normalize_route_set(self.grant_query)
        dl = _normalize_route_set(self.delegation)
        dg = _normalize_route_set(self.delegation_grant)
        dc = _normalize_route_set(self.decision)
        sc = _normalize_route_set(self.scope)

        has_registrations = bool(pr or ra or gq or dl or dg or dc or sc)

        if not has_registrations:
            return Deps()

        if self.kernel is None:
            raise exc.internal("kernel is required when registering authz dependency routes")

        shared = build_authz_shared_services(self.kernel)

        merged: Deps = Deps()

        if pr:
            merged = merged.merge(
                Deps.routed(
                    {
                        PrincipalRegistryDepKey: {
                            name: ConfigurablePrincipalRegistry() for name in pr
                        },
                    },
                ),
            )

        if ra:
            merged = merged.merge(
                Deps.routed(
                    {
                        RoleAssignmentDepKey: {name: ConfigurableRoleAssignment() for name in ra},
                    },
                ),
            )

        if gq:
            merged = merged.merge(
                Deps.routed(
                    {
                        GrantQueryDepKey: {name: ConfigurableGrantQuery() for name in gq},
                    },
                ),
            )

        if dl:
            merged = merged.merge(
                Deps.routed(
                    {
                        DelegationDepKey: {name: ConfigurableDelegationQuery() for name in dl},
                    },
                ),
            )

        if dg:
            merged = merged.merge(
                Deps.routed(
                    {
                        DelegationGrantDepKey: {name: ConfigurableDelegationGrant() for name in dg},
                    },
                ),
            )

        if dc:
            merged = merged.merge(
                Deps.routed(
                    {
                        AuthzDecisionDepKey: {
                            name: ConfigurableAuthzDecision(shared=shared) for name in dc
                        },
                    },
                ),
            )

        if sc:
            merged = merged.merge(
                Deps.routed(
                    {
                        AuthzScopeDepKey: {
                            name: ConfigurableAuthzScope(shared=shared) for name in sc
                        },
                    },
                ),
            )

        return merged
