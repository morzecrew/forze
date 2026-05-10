"""Authn dependency configurations, factories, and module."""

from .configs import (
    AuthnKernelConfig,
    AuthnSharedServices,
    build_authn_shared_services,
)
from .deps import (
    ConfigurableApiKeyLifecycle,
    ConfigurableArgon2PasswordVerifier,
    ConfigurableAuthn,
    ConfigurableDeterministicUuidResolver,
    ConfigurableForzeJwtTokenVerifier,
    ConfigurableHmacApiKeyVerifier,
    ConfigurableJwtNativeUuidResolver,
    ConfigurableMappingTableResolver,
    ConfigurablePasswordAccountProvisioning,
    ConfigurablePasswordLifecycle,
    ConfigurableTokenLifecycle,
)
from .module import AuthnDepsModule

# ----------------------- #

__all__ = [
    "AuthnDepsModule",
    "AuthnKernelConfig",
    "AuthnSharedServices",
    "build_authn_shared_services",
    "ConfigurableApiKeyLifecycle",
    "ConfigurableArgon2PasswordVerifier",
    "ConfigurableAuthn",
    "ConfigurableDeterministicUuidResolver",
    "ConfigurableForzeJwtTokenVerifier",
    "ConfigurableHmacApiKeyVerifier",
    "ConfigurableJwtNativeUuidResolver",
    "ConfigurableMappingTableResolver",
    "ConfigurablePasswordAccountProvisioning",
    "ConfigurablePasswordLifecycle",
    "ConfigurableTokenLifecycle",
]
