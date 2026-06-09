"""Telegram Login OIDC verifier factory."""

from typing import final

import attrs

from forze.application.contracts.authn import AuthnSpec, TokenVerifierPort
from forze.application.execution import ExecutionContext

from forze_identity.oidc import ConfigurableOidcIdpVerifier
from .config import TelegramLoginOidcConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableTelegramLoginOidcVerifier:
    """Build a Telegram Login-configured :class:`OidcTokenVerifier`."""

    config: TelegramLoginOidcConfig
    """Telegram OIDC client settings."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> TokenVerifierPort:
        return ConfigurableOidcIdpVerifier(preset=self.config.to_preset())(ctx, spec)
