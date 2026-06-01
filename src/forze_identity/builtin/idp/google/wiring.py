"""Convenience wiring for Google OIDC bootstrap auth."""

from forze.application.execution import Deps

from .._oidc import oidc_bootstrap_identity_deps
from .config import GoogleOidcConfig
from .deps import ConfigurableGoogleOidcVerifier

# ----------------------- #


def google_identity_deps(
    config: GoogleOidcConfig,
    *,
    authn_route: str = "bootstrap",
) -> Deps:
    """Register a bootstrap route that accepts Google ``id_token`` JWTs only."""

    return oidc_bootstrap_identity_deps(
        authn_route=authn_route,
        token_verifier=ConfigurableGoogleOidcVerifier(config=config),
    )
