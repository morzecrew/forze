"""Convenience wiring for Telegram Login OIDC bootstrap auth."""

from forze.application.execution import Deps

from .._oidc import oidc_bootstrap_identity_deps
from .config import TelegramLoginOidcConfig
from .deps import ConfigurableTelegramLoginOidcVerifier

# ----------------------- #


def telegram_login_identity_deps(
    config: TelegramLoginOidcConfig,
    *,
    authn_route: str = "bootstrap",
) -> Deps:
    """Register a bootstrap route that accepts Telegram Login ``id_token`` JWTs only."""

    return oidc_bootstrap_identity_deps(
        authn_route=authn_route,
        token_verifier=ConfigurableTelegramLoginOidcVerifier(config=config),
    )
