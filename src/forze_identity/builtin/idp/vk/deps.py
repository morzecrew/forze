"""VK ID verifier factory (``public_info`` introspection)."""

from typing import final

import attrs

from forze.application.contracts.authn import AuthnSpec, TokenVerifierPort
from forze.application.execution import ExecutionContext

from .config import VkIdOidcConfig
from .verifier import VkPublicInfoTokenVerifier

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableVkIdOidcVerifier:
    """Build a VK ID-configured :class:`VkPublicInfoTokenVerifier` (``id_token`` only).

    VK publishes no JWKS, so unlike the Google/Telegram presets this factory does
    not produce a JWT-signature verifier — it produces the server-side
    introspection verifier documented by VK ID.
    """

    config: VkIdOidcConfig
    """VK client id, introspection, and token endpoint settings."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> TokenVerifierPort:
        _ = ctx, spec
        return VkPublicInfoTokenVerifier(
            client_id=self.config.client_id,
            public_info_url=self.config.public_info_endpoint,
            issuer=self.config.issuer,
            timeout=self.config.verify_timeout,
        )
