from ..base import BaseDepPort, DepKey
from .ports import (
    ApiKeyLifecyclePort,
    AuthnPort,
    PasswordAccountProvisioningPort,
    PasswordLifecyclePort,
    TokenLifecyclePort,
)
from .specs import AuthnSpec

# ----------------------- #

AuthnDepPort = BaseDepPort[AuthnSpec, AuthnPort]
"""Authentication dependency port."""

PasswordLifecycleDepPort = BaseDepPort[AuthnSpec, PasswordLifecyclePort]
"""Password lifecycle dependency port."""

TokenLifecycleDepPort = BaseDepPort[AuthnSpec, TokenLifecyclePort]
"""Token lifecycle dependency port."""

ApiKeyLifecycleDepPort = BaseDepPort[AuthnSpec, ApiKeyLifecyclePort]
"""API key lifecycle dependency port."""

PasswordAccountProvisioningDepPort = BaseDepPort[
    AuthnSpec, PasswordAccountProvisioningPort
]
"""Password account provisioning dependency port."""

# ....................... #

AuthnDepKey = DepKey[AuthnDepPort]("authn")
"""Key used to register the `AuthnPort` builder implementation."""

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
