from ..base import BaseDepPort, DepKey
from .ports import (
    ApiKeyLifecyclePort,
    AuthenticationPort,
    AuthorizationPort,
    PasswordLifecyclePort,
    TokenLifecyclePort,
)
from .specs import AuthSpec

# ----------------------- #

AuthenticationDepPort = BaseDepPort[AuthSpec, AuthenticationPort]
"""Authentication dependency port."""

AuthorizationDepPort = BaseDepPort[AuthSpec, AuthorizationPort]
"""Authorization dependency port."""

TokenLifecycleDepPort = BaseDepPort[AuthSpec, TokenLifecyclePort]
"""Token lifecycle dependency port."""

ApiKeyLifecycleDepPort = BaseDepPort[AuthSpec, ApiKeyLifecyclePort]
"""API key lifecycle dependency port."""

PasswordLifecycleDepPort = BaseDepPort[AuthSpec, PasswordLifecyclePort]
"""Password lifecycle dependency port."""

# ....................... #

AuthenticationDepKey = DepKey[AuthenticationDepPort]("authentication")
"""Key used to register the `AuthenticationPort` builder implementation."""

AuthorizationDepKey = DepKey[AuthorizationDepPort]("authorization")
"""Key used to register the `AuthorizationPort` builder implementation."""

TokenLifecycleDepKey = DepKey[TokenLifecycleDepPort]("token_lifecycle")
"""Key used to register the `AuthTokenLifecyclePort` builder implementation."""

ApiKeyLifecycleDepKey = DepKey[ApiKeyLifecycleDepPort]("api_key_lifecycle")
"""Key used to register the `ApiKeyLifecyclePort` builder implementation."""

PasswordLifecycleDepKey = DepKey[PasswordLifecycleDepPort]("password_lifecycle")
"""Key used to register the `PasswordLifecyclePort` builder implementation."""
