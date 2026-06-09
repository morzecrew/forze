"""Document-backed authentication (password, tokens, API keys).

The package decomposes the classic ``AuthnAdapter`` into:

* :mod:`forze_authn.verifiers` — credential-specific verifiers producing
  :class:`~forze.application.contracts.authn.VerifiedAssertion` instances.
* :mod:`forze_authn.resolvers` — principal resolvers turning assertions into canonical
  :class:`~forze.application.contracts.authn.AuthnIdentity` values.
* :mod:`forze_authn.orchestrator` — the :class:`AuthnPort` facade composing the above.

External IdP integrations (forze_oidc, forze_firebase_auth, forze_casdoor) ship their own
verifiers and reuse the resolvers here, then plug into the same dep keys via overrides on
:class:`AuthnDepsModule`.
"""

from .execution import (
    AuthnDepsModule,
    AuthnKernelConfig,
    AuthnSharedServices,
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
    build_authn_shared_services,
)
from .orchestrator import AuthnOrchestrator
from .resolvers import (
    DeterministicUuidResolver,
    JwtNativeUuidResolver,
    MappingTableResolver,
)
from .verifiers import (
    Argon2PasswordVerifier,
    ForzeJwtTokenVerifier,
    HmacApiKeyVerifier,
)

# ----------------------- #

__all__ = [
    "Argon2PasswordVerifier",
    "AuthnDepsModule",
    "AuthnKernelConfig",
    "AuthnOrchestrator",
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
    "DeterministicUuidResolver",
    "ForzeJwtTokenVerifier",
    "HmacApiKeyVerifier",
    "JwtNativeUuidResolver",
    "MappingTableResolver",
]
