"""Authn dependency configurations, factories, and module."""

from .configs import (
    ApiKeyLifecycleRouteConfig,
    AuthnRouteConfig,
    PasswordLifecycleRouteConfig,
    PasswordProvisioningRouteConfig,
    TokenLifecycleRouteConfig,
)
from .deps import (
    ConfigurableApiKeyLifecycle,
    ConfigurableAuthn,
    ConfigurablePasswordAccountProvisioning,
    ConfigurablePasswordLifecycle,
    ConfigurableTokenLifecycle,
)
from .module import AuthnDepsModule

# ----------------------- #

__all__ = [
    "AuthnDepsModule",
    "AuthnRouteConfig",
    "TokenLifecycleRouteConfig",
    "PasswordLifecycleRouteConfig",
    "ApiKeyLifecycleRouteConfig",
    "PasswordProvisioningRouteConfig",
    "ConfigurableAuthn",
    "ConfigurableTokenLifecycle",
    "ConfigurablePasswordLifecycle",
    "ConfigurableApiKeyLifecycle",
    "ConfigurablePasswordAccountProvisioning",
]
