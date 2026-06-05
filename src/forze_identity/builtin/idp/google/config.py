"""Google Sign-In OIDC preset configuration."""

from datetime import timedelta
from typing import Final, final

import attrs

from forze.base.exceptions import exc
from forze_identity.oidc import OidcIdpPreset

# ----------------------- #

GOOGLE_OIDC_ISSUER: Final[str] = "https://accounts.google.com"
GOOGLE_OIDC_JWKS_URI: Final[str] = "https://www.googleapis.com/oauth2/v3/certs"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GoogleOidcConfig:
    """Google OIDC verifier settings (audience = OAuth client id)."""

    client_id: str
    """Google OAuth 2.0 client id (used as JWT ``aud``)."""

    tenant_claim: str | None = attrs.field(default=None)
    """Optional claim for ``issuer_tenant_hint`` (uncommon for Google)."""

    leeway: timedelta = attrs.field(default=timedelta(seconds=10))
    """JWT clock-skew leeway."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.leeway.total_seconds() <= 0:
            raise exc.configuration("Leeway must be positive")

    # ....................... #

    def to_preset(self) -> OidcIdpPreset:
        return OidcIdpPreset(
            issuer=GOOGLE_OIDC_ISSUER,
            jwks_uri=GOOGLE_OIDC_JWKS_URI,
            audience=self.client_id,
            leeway=self.leeway,
            tenant_claim=self.tenant_claim,
        )
