"""VK ID preset configuration (code exchange + ``public_info`` introspection)."""

from typing import Final, final

import attrs
from pydantic import SecretStr

# ----------------------- #

VK_ID_OIDC_ISSUER: Final[str] = "https://id.vk.ru"
VK_ID_TOKEN_ENDPOINT: Final[str] = "https://id.vk.ru/oauth2/auth"
VK_ID_PUBLIC_INFO_ENDPOINT: Final[str] = "https://id.vk.ru/oauth2/public_info"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class VkIdOidcConfig:
    """VK ID OAuth client settings for code exchange and ``id_token`` verification.

    VK publishes no JWKS, so ``id_token`` verification uses server-side
    introspection at ``public_info_endpoint`` instead of local signature checks
    (see :class:`~forze_identity.builtin.idp.vk.verifier.VkPublicInfoTokenVerifier`).
    """

    client_id: str
    """VK application id (sent as ``client_id`` on exchange and introspection)."""

    redirect_uri: str
    """Registered redirect URI (must match authorization request)."""

    client_secret: str | SecretStr | None = attrs.field(default=None, repr=False)
    """Optional client secret (PKCE-only apps may omit)."""

    token_endpoint: str = attrs.field(default=VK_ID_TOKEN_ENDPOINT)
    """OAuth token endpoint for authorization-code exchange."""

    issuer: str = attrs.field(default=VK_ID_OIDC_ISSUER)
    """Issuer recorded on verified assertions (principal-resolver discriminator)."""

    public_info_endpoint: str = attrs.field(default=VK_ID_PUBLIC_INFO_ENDPOINT)
    """VK ID ``public_info`` endpoint used for server-side ``id_token`` introspection."""

    verify_timeout: float = attrs.field(default=10.0)
    """Request timeout in seconds for the introspection call."""

    # ....................... #

    def client_secret_value(self) -> str | None:
        if self.client_secret is None:
            return None

        if isinstance(self.client_secret, SecretStr):
            return self.client_secret.get_secret_value()

        return self.client_secret
