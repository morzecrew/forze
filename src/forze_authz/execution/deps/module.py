"""Authz dependency module for the application kernel."""

from collections.abc import Collection
from enum import StrEnum
from typing import final

import attrs

from forze.application.contracts.authz import (
    AuthzDecisionDepKey,
    AuthzScopeDepKey,
    GrantQueryDepKey,
    PrincipalRegistryDepKey,
    RoleAssignmentDepKey,
)
from forze.application.execution import Deps, DepsModule
from forze.base.exceptions import exc

from .configs import AuthzKernelConfig, build_authz_shared_services
from .deps import (
    ConfigurableAuthzDecision,
    ConfigurableAuthzScope,
    ConfigurableGrantQuery,
    ConfigurablePrincipalRegistry,
    ConfigurableRoleAssignment,
)

# ----------------------- #


def _normalize_route_set[K: str | StrEnum](
    routes: Collection[K] | None,
) -> frozenset[K]:
    return frozenset(routes) if routes else frozenset()


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzDepsModule[K: str | StrEnum](DepsModule[K]):
    """Registers authz dependency factories that resolve document ports via execution context."""

    kernel: AuthzKernelConfig | None = attrs.field(default=None)
    principal_registry: Collection[K] | None = attrs.field(default=None)
    role_assignment: Collection[K] | None = attrs.field(default=None)
    grant_query: Collection[K] | None = attrs.field(default=None)
    decision: Collection[K] | None = attrs.field(default=None)
    scope: Collection[K] | None = attrs.field(default=None)

    def __call__(self) -> Deps[K]:
        pr = _normalize_route_set(self.principal_registry)
        ra = _normalize_route_set(self.role_assignment)
        gq = _normalize_route_set(self.grant_query)
        dc = _normalize_route_set(self.decision)
        sc = _normalize_route_set(self.scope)

        has_registrations = bool(pr or ra or gq or dc or sc)

        if not has_registrations:
            return Deps[K]()

        if self.kernel is None:
            raise exc.internal(
                "kernel is required when registering authz dependency routes"
            )

        shared = build_authz_shared_services(self.kernel)

        merged: Deps[K] = Deps[K]()

        if pr:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        PrincipalRegistryDepKey: {
                            name: ConfigurablePrincipalRegistry() for name in pr
                        },
                    },
                ),
            )

        if ra:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        RoleAssignmentDepKey: {
                            name: ConfigurableRoleAssignment() for name in ra
                        },
                    },
                ),
            )

        if gq:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        GrantQueryDepKey: {
                            name: ConfigurableGrantQuery() for name in gq
                        },
                    },
                ),
            )

        if dc:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        AuthzDecisionDepKey: {
                            name: ConfigurableAuthzDecision(shared=shared)
                            for name in dc
                        },
                    },
                ),
            )

        if sc:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        AuthzScopeDepKey: {
                            name: ConfigurableAuthzScope(shared=shared) for name in sc
                        },
                    },
                ),
            )

        return merged
