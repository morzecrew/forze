"""Authorization helpers: policy principals, document-backed RBAC, execution wiring."""

from .application import (
    AuthzResourceName,
    delegation_grant_spec,
    policy_principal_spec,
)
from .execution import (
    AuthzDepsModule,
    AuthzKernelConfig,
    AuthzSharedServices,
    ConfigurableAuthzDecision,
    ConfigurableAuthzScope,
    ConfigurableDelegationGrant,
    ConfigurableDelegationQuery,
    ConfigurableGrantQuery,
    ConfigurablePrincipalRegistry,
    ConfigurableRoleAssignment,
    build_authz_shared_services,
)

# ----------------------- #

__all__ = [
    "AuthzDepsModule",
    "AuthzKernelConfig",
    "AuthzResourceName",
    "AuthzSharedServices",
    "ConfigurableAuthzDecision",
    "ConfigurableAuthzScope",
    "ConfigurableDelegationGrant",
    "ConfigurableDelegationQuery",
    "ConfigurableGrantQuery",
    "ConfigurablePrincipalRegistry",
    "ConfigurableRoleAssignment",
    "build_authz_shared_services",
    "delegation_grant_spec",
    "policy_principal_spec",
]
