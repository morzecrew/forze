from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import (
    AuthzDecisionPort,
    AuthzScopePort,
    DelegationGrantPort,
    DelegationPort,
    GrantQueryPort,
    PrincipalRegistryPort,
    RoleAssignmentPort,
)
from .specs import AuthzSpec

# ----------------------- #

AuthzDecisionDepPort = ConfigurableDepPort[AuthzSpec, AuthzDecisionPort]
GrantQueryDepPort = ConfigurableDepPort[AuthzSpec, GrantQueryPort]
DelegationDepPort = ConfigurableDepPort[AuthzSpec, DelegationPort]
DelegationGrantDepPort = ConfigurableDepPort[AuthzSpec, DelegationGrantPort]
PrincipalRegistryDepPort = ConfigurableDepPort[AuthzSpec, PrincipalRegistryPort]
RoleAssignmentDepPort = ConfigurableDepPort[AuthzSpec, RoleAssignmentPort]
AuthzScopeDepPort = ConfigurableDepPort[AuthzSpec, AuthzScopePort]

# ....................... #

AuthzDecisionDepKey = DepKey[AuthzDecisionDepPort]("authz_decision")
GrantQueryDepKey = DepKey[GrantQueryDepPort]("authz_grant_query")
DelegationDepKey = DepKey[DelegationDepPort]("authz_delegation")
DelegationGrantDepKey = DepKey[DelegationGrantDepPort]("authz_delegation_grant")
PrincipalRegistryDepKey = DepKey[PrincipalRegistryDepPort]("authz_principal_registry")
RoleAssignmentDepKey = DepKey[RoleAssignmentDepPort]("authz_role_assignment")
AuthzScopeDepKey = DepKey[AuthzScopeDepPort]("authz_scope")

# ....................... #


class AuthzDeps(ConvenientDeps):
    """Convenience wrapper for authorization dependencies."""

    def decision(self, spec: AuthzSpec) -> AuthzDecisionPort:
        """Resolve the decision port for ``spec``."""

        return self._resolve_configurable(AuthzDecisionDepKey, spec, route=spec.name)

    # ....................... #

    def grant_query(self, spec: AuthzSpec) -> GrantQueryPort:
        """Resolve the grant query port for ``spec``."""

        return self._resolve_configurable(GrantQueryDepKey, spec, route=spec.name)

    # ....................... #

    def delegation(self, spec: AuthzSpec) -> DelegationPort:
        """Resolve the delegation (``may_act``) query port for ``spec``."""

        return self._resolve_configurable(DelegationDepKey, spec, route=spec.name)

    # ....................... #

    def delegation_grant(self, spec: AuthzSpec) -> DelegationGrantPort:
        """Resolve the delegation grant port for ``spec`` (a write — guarded)."""

        return self._resolve_command(DelegationGrantDepKey, spec, route=spec.name)

    # ....................... #

    def scope(self, spec: AuthzSpec) -> AuthzScopePort:
        """Resolve the data scoping port for ``spec``."""

        return self._resolve_configurable(AuthzScopeDepKey, spec, route=spec.name)

    # ....................... #

    def principal_registry(self, spec: AuthzSpec) -> PrincipalRegistryPort:
        """Resolve the principal registry port for ``spec`` (a write — guarded)."""

        return self._resolve_command(
            PrincipalRegistryDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def role_assignment(self, spec: AuthzSpec) -> RoleAssignmentPort:
        """Resolve the role assignment port for ``spec`` (a write — guarded)."""

        return self._resolve_command(RoleAssignmentDepKey, spec, route=spec.name)
