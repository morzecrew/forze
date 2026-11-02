"""Authorization helpers: policy principals, document-backed RBAC, execution wiring."""

from forze.application.integrations.authz import ConfigGrants, ConfigGrantsProvider

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
    grants_invalidation_lifecycle_step,
    invalidate_grants,
    permission_providers_lifecycle_step,
)
from .observability import instrument_grants_invalidation
from .services import GrantsCache, GrantsInvalidation

# ----------------------- #

__all__ = [
    "AuthzDepsModule",
    "AuthzKernelConfig",
    "AuthzResourceName",
    "AuthzSharedServices",
    "ConfigGrants",
    "ConfigGrantsProvider",
    "ConfigurableAuthzDecision",
    "ConfigurableAuthzScope",
    "ConfigurableDelegationGrant",
    "ConfigurableDelegationQuery",
    "ConfigurableGrantQuery",
    "ConfigurablePrincipalRegistry",
    "ConfigurableRoleAssignment",
    "GrantsCache",
    "GrantsInvalidation",
    "build_authz_shared_services",
    "grants_invalidation_lifecycle_step",
    "instrument_grants_invalidation",
    "invalidate_grants",
    "permission_providers_lifecycle_step",
    "delegation_grant_spec",
    "policy_principal_spec",
]
