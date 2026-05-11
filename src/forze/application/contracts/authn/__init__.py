"""Authentication contracts: orchestration port, verifier ports, principal resolver, and value objects.

Lifecycle and provisioning ports live in sibling contract groups
(:mod:`forze.application.contracts.authn_lifecycle`,
:mod:`forze.application.contracts.authn_provisioning`) so external IdP integrations can
implement only what they care about.
"""

from .deps import (
    ApiKeyLifecycleDepKey,
    ApiKeyLifecycleDepPort,
    ApiKeyVerifierDepKey,
    ApiKeyVerifierDepPort,
    AuthnDepKey,
    AuthnDepPort,
    PasswordAccountProvisioningDepKey,
    PasswordAccountProvisioningDepPort,
    PasswordLifecycleDepKey,
    PasswordLifecycleDepPort,
    PasswordVerifierDepKey,
    PasswordVerifierDepPort,
    PrincipalResolverDepKey,
    PrincipalResolverDepPort,
    TokenLifecycleDepKey,
    TokenLifecycleDepPort,
    TokenVerifierDepKey,
    TokenVerifierDepPort,
)
from .ports import (
    ApiKeyLifecyclePort,
    ApiKeyVerifierPort,
    AuthnPort,
    PasswordAccountProvisioningPort,
    PasswordLifecyclePort,
    PasswordVerifierPort,
    PrincipalResolverPort,
    TokenLifecyclePort,
    TokenVerifierPort,
)
from .specs import AuthnMethod, AuthnSpec
from .value_objects import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    AuthnIdentity,
    CredentialLifetime,
    IssuedAccessToken,
    IssuedApiKey,
    IssuedRefreshToken,
    IssuedTokens,
    PasswordCredentials,
    RefreshTokenCredentials,
    VerifiedAssertion,
)

# ----------------------- #

__all__ = [
    "AccessTokenCredentials",
    "ApiKeyCredentials",
    "ApiKeyLifecycleDepKey",
    "ApiKeyLifecycleDepPort",
    "ApiKeyLifecyclePort",
    "ApiKeyVerifierDepKey",
    "ApiKeyVerifierDepPort",
    "ApiKeyVerifierPort",
    "AuthnDepKey",
    "AuthnDepPort",
    "AuthnIdentity",
    "AuthnMethod",
    "AuthnPort",
    "AuthnSpec",
    "CredentialLifetime",
    "IssuedAccessToken",
    "IssuedApiKey",
    "IssuedRefreshToken",
    "IssuedTokens",
    "PasswordAccountProvisioningDepKey",
    "PasswordAccountProvisioningDepPort",
    "PasswordAccountProvisioningPort",
    "PasswordCredentials",
    "PasswordLifecycleDepKey",
    "PasswordLifecycleDepPort",
    "PasswordLifecyclePort",
    "PasswordVerifierDepKey",
    "PasswordVerifierDepPort",
    "PasswordVerifierPort",
    "PrincipalResolverDepKey",
    "PrincipalResolverDepPort",
    "PrincipalResolverPort",
    "RefreshTokenCredentials",
    "TokenLifecycleDepKey",
    "TokenLifecycleDepPort",
    "TokenLifecyclePort",
    "TokenVerifierDepKey",
    "TokenVerifierDepPort",
    "TokenVerifierPort",
    "VerifiedAssertion",
]
