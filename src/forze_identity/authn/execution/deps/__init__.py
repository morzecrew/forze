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
    ConfigurableLoggingAuthnEventSink,
    ConfigurableMappingTableResolver,
    ConfigurablePasswordAccountProvisioning,
    ConfigurablePasswordLifecycle,
    ConfigurablePasswordReset,
    ConfigurablePolicyPrincipalEligibility,
    ConfigurablePrincipalDeactivation,
    ConfigurableTokenLifecycle,
)
from .module import AuthnDepsModule, EligibilityChoice

# ----------------------- #

__all__ = [
    "AuthnDepsModule",
    "EligibilityChoice",
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
    "ConfigurableLoggingAuthnEventSink",
    "ConfigurableMappingTableResolver",
    "ConfigurablePasswordAccountProvisioning",
    "ConfigurablePasswordLifecycle",
    "ConfigurablePasswordReset",
    "ConfigurablePolicyPrincipalEligibility",
    "ConfigurablePrincipalDeactivation",
    "ConfigurableTokenLifecycle",
]
