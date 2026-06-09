from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import (
    ApiKeyLifecyclePort,
    ApiKeyVerifierPort,
    AuthnPort,
    PasswordAccountProvisioningPort,
    PasswordLifecyclePort,
    PasswordVerifierPort,
    PrincipalDeactivationPort,
    PrincipalEligibilityPort,
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

PrincipalEligibilityDepPort = ConfigurableDepPort[AuthnSpec, PrincipalEligibilityPort]
"""Principal eligibility dependency port."""

PrincipalDeactivationDepPort = ConfigurableDepPort[AuthnSpec, PrincipalDeactivationPort]
"""Principal deactivation dependency port."""

# ....................... #

PrincipalEligibilityDepKey = DepKey[PrincipalEligibilityDepPort]("authn_principal_eligibility")
"""Key used to register the `PrincipalEligibilityPort` builder implementation."""

PrincipalDeactivationDepKey = DepKey[PrincipalDeactivationDepPort](
    "authn_principal_deactivation"
)
"""Key used to register the `PrincipalDeactivationPort` builder implementation."""

# ....................... #


class AuthnDeps(ConvenientDeps):
    """Convenience wrapper for authentication dependencies."""

    def authn(self, spec: AuthnSpec) -> AuthnPort:
        """Resolve the authentication orchestrator for ``spec``."""

        return self._resolve_configurable(AuthnDepKey, spec, route=spec.name)

    def eligibility(self, spec: AuthnSpec) -> PrincipalEligibilityPort:
        """Resolve principal eligibility checks for ``spec``."""

        return self._resolve_configurable(
            PrincipalEligibilityDepKey,
            spec,
            route=spec.name,
        )

    def token_lifecycle(self, spec: AuthnSpec) -> TokenLifecyclePort:
        """Resolve token lifecycle for ``spec`` (a write — guarded)."""

        return self._resolve_command(TokenLifecycleDepKey, spec, route=spec.name)

    def password_lifecycle(self, spec: AuthnSpec) -> PasswordLifecyclePort:
        """Resolve password lifecycle for ``spec`` (a write — guarded)."""

        return self._resolve_command(
            PasswordLifecycleDepKey,
            spec,
            route=spec.name,
        )

    def api_key_lifecycle(self, spec: AuthnSpec) -> ApiKeyLifecyclePort:
        """Resolve API key lifecycle for ``spec`` (a write — guarded)."""

        return self._resolve_command(ApiKeyLifecycleDepKey, spec, route=spec.name)

    def password_account_provisioning(
        self,
        spec: AuthnSpec,
    ) -> PasswordAccountProvisioningPort:
        """Resolve password account provisioning for ``spec`` (a write — guarded)."""

        return self._resolve_command(
            PasswordAccountProvisioningDepKey,
            spec,
            route=spec.name,
        )

    def principal_deactivation(self, spec: AuthnSpec) -> PrincipalDeactivationPort:
        """Resolve cascaded principal deactivation for ``spec`` (a write — guarded)."""

        return self._resolve_command(
            PrincipalDeactivationDepKey,
            spec,
            route=spec.name,
        )
