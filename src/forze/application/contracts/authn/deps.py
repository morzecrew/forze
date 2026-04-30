from ..base import BaseDepPort, DepKey
from .ports import AuthnPort, PasswordLifecyclePort, TokenLifecyclePort
from .specs import AuthnSpec

# ----------------------- #

AuthnDepPort = BaseDepPort[AuthnSpec, AuthnPort]
"""Authentication dependency port."""

PasswordLifecycleDepPort = BaseDepPort[AuthnSpec, PasswordLifecyclePort]
"""Password lifecycle dependency port."""

TokenLifecycleDepPort = BaseDepPort[AuthnSpec, TokenLifecyclePort]
"""Token lifecycle dependency port."""

# ....................... #

AuthnDepKey = DepKey[AuthnDepPort]("authn")
"""Key used to register the `AuthnPort` builder implementation."""

PasswordLifecycleDepKey = DepKey[PasswordLifecycleDepPort]("authn_password_lifecycle")
"""Key used to register the `PasswordLifecyclePort` builder implementation."""

TokenLifecycleDepKey = DepKey[TokenLifecycleDepPort]("authn_token_lifecycle")
"""Key used to register the `TokenLifecyclePort` builder implementation."""
