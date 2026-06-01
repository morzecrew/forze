"""Google Sign-In OIDC preset (:func:`google_identity_deps`)."""

from .._compat import require_oidc

require_oidc()

# ....................... #

from .config import GOOGLE_OIDC_ISSUER, GOOGLE_OIDC_JWKS_URI, GoogleOidcConfig
from .deps import ConfigurableGoogleOidcVerifier
from .wiring import google_identity_deps

# ----------------------- #

__all__ = [
    "ConfigurableGoogleOidcVerifier",
    "GOOGLE_OIDC_ISSUER",
    "GOOGLE_OIDC_JWKS_URI",
    "GoogleOidcConfig",
    "google_identity_deps",
]
