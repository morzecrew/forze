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
    permission_providers_lifecycle_step,
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
    "permission_providers_lifecycle_step",
    "delegation_grant_spec",
    "policy_principal_spec",
]
