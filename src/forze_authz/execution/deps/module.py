"""Authz dependency module for the application kernel."""

from collections.abc import Collection
from enum import StrEnum
from typing import final

import attrs

from forze.application.contracts.authz import (
    AuthzDepKey,
    EffectiveGrantsDepKey,
    PrincipalRegistryDepKey,
    RoleAssignmentDepKey,
)
from forze.application.execution import Deps, DepsModule
from forze.base.errors import CoreError

from .configs import AuthzKernelConfig, build_authz_shared_services
from .deps import (
    ConfigurableAuthz,
    ConfigurableEffectiveGrants,
    ConfigurablePrincipalRegistry,
    ConfigurableRoleAssignment,
)

# ----------------------- #


def _normalize_route_set[K: str | StrEnum](
    routes: Collection[K] | None,
) -> frozenset[K]:
    return frozenset(routes) if routes else frozenset()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzDepsModule[K: str | StrEnum](DepsModule[K]):
    """Registers authz dependency factories that resolve document ports via execution context."""

    kernel: AuthzKernelConfig | None = attrs.field(default=None)
    """Kernel configuration; required when any authz route registration is non-empty."""

    principal_registry: Collection[K] | None = attrs.field(default=None)
    role_assignment: Collection[K] | None = attrs.field(default=None)
    effective_grants: Collection[K] | None = attrs.field(default=None)
    authz: Collection[K] | None = attrs.field(default=None)

    # ....................... #

    def __call__(self) -> Deps[K]:
        pr = _normalize_route_set(self.principal_registry)
        ra = _normalize_route_set(self.role_assignment)
        eg = _normalize_route_set(self.effective_grants)
        az = _normalize_route_set(self.authz)

        has_registrations = bool(pr or ra or eg or az)

        if not has_registrations:
            return Deps[K]()

        if self.kernel is None:
            msg = "kernel is required when registering authz dependency routes"

            raise CoreError(msg)

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

        if eg:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        EffectiveGrantsDepKey: {
                            name: ConfigurableEffectiveGrants() for name in eg
                        },
                    },
                ),
            )

        if az:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        AuthzDepKey: {
                            name: ConfigurableAuthz(shared=shared) for name in az
                        },
                    },
                ),
            )

        return merged
