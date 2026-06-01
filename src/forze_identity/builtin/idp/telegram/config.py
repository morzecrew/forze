"""Telegram Login OIDC preset configuration."""

import base64
from datetime import timedelta
from typing import Final, final

import attrs
from pydantic import SecretStr

from forze_identity.oidc import OidcIdpPreset

# ----------------------- #

TELEGRAM_LOGIN_OIDC_ISSUER: Final[str] = "https://oauth.telegram.org"
TELEGRAM_LOGIN_JWKS_URI: Final[str] = "https://oauth.telegram.org/.well-known/jwks.json"
TELEGRAM_LOGIN_TOKEN_ENDPOINT: Final[str] = "https://oauth.telegram.org/token"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TelegramLoginOidcConfig:
    """Telegram Login OIDC client (BotFather Client ID + Secret)."""

    client_id: str
    """Bot Client ID from @BotFather (JWT ``aud``)."""

    client_secret: str | SecretStr = attrs.field(repr=False)
    """Client Secret from @BotFather."""

    redirect_uri: str
    """Registered redirect URI for the authorization-code flow."""

    token_endpoint: str = attrs.field(default=TELEGRAM_LOGIN_TOKEN_ENDPOINT)
    """OAuth token endpoint for authorization-code exchange."""

    issuer: str = attrs.field(default=TELEGRAM_LOGIN_OIDC_ISSUER)
    """Expected ``iss`` on Telegram ``id_token``."""

    jwks_uri: str = attrs.field(default=TELEGRAM_LOGIN_JWKS_URI)
    """JWKS URI for ``id_token`` signature verification."""

    tenant_claim: str | None = attrs.field(default=None)
    """Optional claim mapped to ``issuer_tenant_hint``."""

    leeway: timedelta = attrs.field(default=timedelta(seconds=10))
    """Clock-skew leeway for JWT validation."""

    # ....................... #

    def client_secret_value(self) -> str:
        if isinstance(self.client_secret, SecretStr):
            return self.client_secret.get_secret_value()

        return self.client_secret

    # ....................... #

    def basic_auth_header(self) -> str:
        raw = f"{self.client_id}:{self.client_secret_value()}".encode()
        return base64.b64encode(raw).decode("ascii")

    # ....................... #

    def to_preset(self) -> OidcIdpPreset:
        return OidcIdpPreset(
            issuer=self.issuer,
            jwks_uri=self.jwks_uri,
            audience=self.client_id,
            leeway=self.leeway,
            tenant_claim=self.tenant_claim,
        )
