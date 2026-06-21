"""Convenience wiring for VK ID OIDC bootstrap auth."""

from forze.application.contracts.deps import Deps

from .._oidc import oidc_bootstrap_identity_deps
from .config import VkIdOidcConfig
from .deps import ConfigurableVkIdOidcVerifier

# ----------------------- #


def vk_identity_deps(
    config: VkIdOidcConfig,
    *,
    authn_route: str = "bootstrap",
) -> Deps:
    """Register a bootstrap route that accepts VK ``id_token``s only.

    Tokens are validated by server-side introspection at VK's ``public_info``
    endpoint (VK publishes no JWKS for local signature verification).
    """

    return oidc_bootstrap_identity_deps(
        authn_route=authn_route,
        token_verifier=ConfigurableVkIdOidcVerifier(config=config),
    )
