from ..base import BaseDepPort, DepKey
from .ports import (
    ApiKeyLifecyclePort,
    AuthenticationPort,
    AuthorizationPort,
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

# ....................... #

AuthenticationDepKey = DepKey[AuthenticationDepPort]("authentication")
"""Key used to register the :class:`AuthenticationPort` builder implementation."""

TokenLifecycleDepKey = DepKey[TokenLifecycleDepPort]("token_lifecycle")
"""Key used to register the :class:`AuthTokenLifecyclePort` builder implementation."""

ApiKeyLifecycleDepKey = DepKey[ApiKeyLifecycleDepPort]("api_key_lifecycle")
"""Key used to register the :class:`ApiKeyLifecyclePort` builder implementation."""

AuthorizationDepKey = DepKey[AuthorizationDepPort]("authorization")
"""Key used to register the :class:`AuthorizationPort` builder implementation."""
