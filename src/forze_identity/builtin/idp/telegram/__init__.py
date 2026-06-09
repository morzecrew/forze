"""Telegram Login OIDC preset (code+PKCE exchange + ``id_token`` verification)."""

from .._compat import require_oidc

require_oidc()

# ....................... #

from forze_identity.oauth import PkcePair, generate_pkce

from .config import (
    TELEGRAM_LOGIN_JWKS_URI,
    TELEGRAM_LOGIN_OIDC_ISSUER,
    TELEGRAM_LOGIN_TOKEN_ENDPOINT,
    TelegramLoginOidcConfig,
)
from .deps import ConfigurableTelegramLoginOidcVerifier
from .exchange import TelegramTokenResponse, exchange_authorization_code
from .wiring import telegram_login_identity_deps

# ----------------------- #

__all__ = [
    "ConfigurableTelegramLoginOidcVerifier",
    "PkcePair",
    "TELEGRAM_LOGIN_JWKS_URI",
    "TELEGRAM_LOGIN_OIDC_ISSUER",
    "TELEGRAM_LOGIN_TOKEN_ENDPOINT",
    "TelegramLoginOidcConfig",
    "TelegramTokenResponse",
    "exchange_authorization_code",
    "generate_pkce",
    "telegram_login_identity_deps",
]
