"""VK ID OIDC preset (code+PKCE exchange + ``id_token`` verification)."""

from .._compat import require_oidc

require_oidc()

# ....................... #

from forze_identity.oauth import PkcePair, generate_pkce

from .config import (
    VK_ID_JWKS_URI,
    VK_ID_OIDC_ISSUER,
    VK_ID_TOKEN_ENDPOINT,
    VkIdOidcConfig,
)
from .deps import ConfigurableVkIdOidcVerifier
from .exchange import VkTokenResponse, exchange_authorization_code
from .wiring import vk_identity_deps

# ----------------------- #

__all__ = [
    "ConfigurableVkIdOidcVerifier",
    "PkcePair",
    "VK_ID_JWKS_URI",
    "VK_ID_OIDC_ISSUER",
    "VK_ID_TOKEN_ENDPOINT",
    "VkIdOidcConfig",
    "VkTokenResponse",
    "exchange_authorization_code",
    "generate_pkce",
    "vk_identity_deps",
]
