"""VK ID OIDC preset configuration."""

from datetime import timedelta
from typing import Final, final

import attrs
from pydantic import SecretStr

from forze.base.exceptions import exc
from forze_identity.oidc import OidcIdpPreset

# ----------------------- #

VK_ID_OIDC_ISSUER: Final[str] = "https://id.vk.ru"
VK_ID_JWKS_URI: Final[str] = "https://id.vk.ru/.well-known/jwks.json"
VK_ID_TOKEN_ENDPOINT: Final[str] = "https://id.vk.ru/oauth2/auth"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class VkIdOidcConfig:
    """VK ID OAuth client settings for code exchange and ``id_token`` verification."""

    client_id: str
    """VK application id (JWT ``aud``)."""

    redirect_uri: str
    """Registered redirect URI (must match authorization request)."""

    client_secret: str | SecretStr | None = attrs.field(default=None, repr=False)
    """Optional client secret (PKCE-only apps may omit)."""

    token_endpoint: str = attrs.field(default=VK_ID_TOKEN_ENDPOINT)
    """OAuth token endpoint for authorization-code exchange."""

    issuer: str = attrs.field(default=VK_ID_OIDC_ISSUER)
    """Expected ``iss`` on VK ``id_token``."""

    jwks_uri: str = attrs.field(default=VK_ID_JWKS_URI)
    """JWKS URI for ``id_token`` signature verification."""

    tenant_claim: str | None = attrs.field(default=None)
    """Optional claim mapped to ``issuer_tenant_hint``."""

    leeway: timedelta = attrs.field(default=timedelta(seconds=10))
    """Clock-skew leeway for JWT validation."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.leeway.total_seconds() <= 0:
            raise exc.configuration("Leeway must be positive")

    # ....................... #

    def client_secret_value(self) -> str | None:
        if self.client_secret is None:
            return None

        if isinstance(self.client_secret, SecretStr):
            return self.client_secret.get_secret_value()

        return self.client_secret

    # ....................... #

    def to_preset(self) -> OidcIdpPreset:
        return OidcIdpPreset(
            issuer=self.issuer,
            jwks_uri=self.jwks_uri,
            audience=self.client_id,
            leeway=self.leeway,
            tenant_claim=self.tenant_claim,
        )
