from ..base import ConfigurableDepPort, DepKey
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
from .specs import AuthnSpec

# ----------------------- #

AuthnDepPort = ConfigurableDepPort[AuthnSpec, AuthnPort]
"""Authentication dependency port (orchestration facade)."""

PasswordVerifierDepPort = ConfigurableDepPort[AuthnSpec, PasswordVerifierPort]
"""Password verifier dependency port."""

TokenVerifierDepPort = ConfigurableDepPort[AuthnSpec, TokenVerifierPort]
"""Token verifier dependency port (one per profile/IdP)."""

ApiKeyVerifierDepPort = ConfigurableDepPort[AuthnSpec, ApiKeyVerifierPort]
"""API key verifier dependency port."""

PrincipalResolverDepPort = ConfigurableDepPort[AuthnSpec, PrincipalResolverPort]
"""Principal resolver dependency port (one per profile)."""

PasswordLifecycleDepPort = ConfigurableDepPort[AuthnSpec, PasswordLifecyclePort]
"""Password lifecycle dependency port."""

TokenLifecycleDepPort = ConfigurableDepPort[AuthnSpec, TokenLifecyclePort]
"""Token lifecycle dependency port."""

ApiKeyLifecycleDepPort = ConfigurableDepPort[AuthnSpec, ApiKeyLifecyclePort]
"""API key lifecycle dependency port."""

PasswordAccountProvisioningDepPort = ConfigurableDepPort[
    AuthnSpec, PasswordAccountProvisioningPort
]
"""Password account provisioning dependency port."""

# ....................... #

AuthnDepKey = DepKey[AuthnDepPort]("authn")
"""Key used to register the `AuthnPort` builder implementation."""

PasswordVerifierDepKey = DepKey[PasswordVerifierDepPort]("authn_password_verifier")
"""Key used to register a `PasswordVerifierPort` builder implementation."""

TokenVerifierDepKey = DepKey[TokenVerifierDepPort]("authn_token_verifier")
"""Key used to register a `TokenVerifierPort` builder implementation (one per profile/IdP)."""

ApiKeyVerifierDepKey = DepKey[ApiKeyVerifierDepPort]("authn_api_key_verifier")
"""Key used to register an `ApiKeyVerifierPort` builder implementation."""

PrincipalResolverDepKey = DepKey[PrincipalResolverDepPort]("authn_principal_resolver")
"""Key used to register a `PrincipalResolverPort` builder implementation (one per profile)."""

PasswordLifecycleDepKey = DepKey[PasswordLifecycleDepPort]("authn_password_lifecycle")
"""Key used to register the `PasswordLifecyclePort` builder implementation."""

TokenLifecycleDepKey = DepKey[TokenLifecycleDepPort]("authn_token_lifecycle")
"""Key used to register the `TokenLifecyclePort` builder implementation."""

ApiKeyLifecycleDepKey = DepKey[ApiKeyLifecycleDepPort]("authn_api_key_lifecycle")
"""Key used to register the `ApiKeyLifecyclePort` builder implementation."""

PasswordAccountProvisioningDepKey = DepKey[PasswordAccountProvisioningDepPort](
    "authn_password_account_provisioning"
)
"""Key used to register the `PasswordAccountProvisioningPort` builder implementation."""
