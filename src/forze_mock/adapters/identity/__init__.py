"""Mock identity plane adapters."""

from .authn import (
    MockApiKeyLifecyclePort,
    MockApiKeyVerifierPort,
    MockAuthnPort,
    MockPasswordAccountProvisioningPort,
    MockPasswordLifecyclePort,
    MockPasswordVerifierPort,
    MockPrincipalDeactivationPort,
    MockPrincipalEligibilityPort,
    MockPrincipalResolverPort,
    MockTokenLifecyclePort,
    MockTokenVerifierPort,
)
from .authz import (
    MockAuthzDecisionPort,
    MockAuthzScopePort,
    MockDelegationGrantPort,
    MockDelegationPort,
    MockGrantQueryPort,
    MockPrincipalRegistryPort,
    MockRoleAssignmentPort,
)
from .secrets import MockSecretsPort
from .tenancy import MockTenantManagementPort, MockTenantResolverPort

__all__ = [
    "MockSecretsPort",
    "MockPasswordVerifierPort",
    "MockTokenVerifierPort",
    "MockApiKeyVerifierPort",
    "MockPrincipalResolverPort",
    "MockPrincipalEligibilityPort",
    "MockPrincipalDeactivationPort",
    "MockTokenLifecyclePort",
    "MockPasswordLifecyclePort",
    "MockApiKeyLifecyclePort",
    "MockPasswordAccountProvisioningPort",
    "MockAuthnPort",
    "MockPrincipalRegistryPort",
    "MockRoleAssignmentPort",
    "MockGrantQueryPort",
    "MockDelegationGrantPort",
    "MockDelegationPort",
    "MockAuthzDecisionPort",
    "MockAuthzScopePort",
    "MockTenantResolverPort",
    "MockTenantManagementPort",
]
