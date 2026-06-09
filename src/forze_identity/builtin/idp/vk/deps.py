"""VK ID OIDC verifier factory."""

from typing import final

import attrs

from forze.application.contracts.authn import AuthnSpec, TokenVerifierPort
from forze.application.execution import ExecutionContext

from forze_identity.oidc import ConfigurableOidcIdpVerifier
from .config import VkIdOidcConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableVkIdOidcVerifier:
    """Build a VK ID-configured :class:`OidcTokenVerifier` (``id_token`` JWT only)."""

    config: VkIdOidcConfig
    """VK client id, JWKS, and token endpoint settings."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> TokenVerifierPort:
        return ConfigurableOidcIdpVerifier(preset=self.config.to_preset())(ctx, spec)
