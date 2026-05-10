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
    ApiKeyCredentials,
    ApiKeyResponse,
    AuthnIdentity,
    CredentialLifetime,
    OAuth2Tokens,
    OAuth2TokensResponse,
    PasswordCredentials,
    TokenCredentials,
    TokenResponse,
    VerifiedAssertion,
)

# ----------------------- #

__all__ = [
    "ApiKeyCredentials",
    "ApiKeyResponse",
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
    "OAuth2Tokens",
    "OAuth2TokensResponse",
    "PasswordCredentials",
    "PasswordVerifierDepKey",
    "PasswordVerifierDepPort",
    "PasswordVerifierPort",
    "PrincipalResolverDepKey",
    "PrincipalResolverDepPort",
    "PrincipalResolverPort",
    "TokenCredentials",
    "TokenResponse",
    "TokenVerifierDepKey",
    "TokenVerifierDepPort",
    "TokenVerifierPort",
    "VerifiedAssertion",
    "PasswordAccountProvisioningDepKey",
    "PasswordAccountProvisioningDepPort",
    "PasswordLifecycleDepKey",
    "PasswordLifecycleDepPort",
    "TokenLifecycleDepKey",
    "TokenLifecycleDepPort",
    "ApiKeyLifecycleDepKey",
    "ApiKeyLifecycleDepPort",
    "PasswordAccountProvisioningPort",
    "PasswordLifecyclePort",
    "TokenLifecyclePort",
    "ApiKeyLifecyclePort",
]
