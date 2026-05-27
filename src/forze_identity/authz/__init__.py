"""Authorization helpers: policy principals, document-backed RBAC, execution wiring."""

from .application import AuthzResourceName, policy_principal_spec
from .execution import (
    AuthzDepsModule,
    AuthzKernelConfig,
    AuthzSharedServices,
    ConfigurableAuthzDecision,
    ConfigurableAuthzScope,
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
    "ConfigurableGrantQuery",
    "ConfigurablePrincipalRegistry",
    "ConfigurableRoleAssignment",
    "build_authz_shared_services",
    "policy_principal_spec",
]
