"""OIDC IdP preset configuration and verifier factory."""

from collections.abc import Sequence
from datetime import timedelta
from typing import final

import attrs

from forze.application.contracts.authn import AuthnSpec, TokenVerifierPort
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from .claims import OidcClaimMapper
from .keys import JwksKeyProvider
from .verifier import OidcTokenVerifier

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OidcIdpPreset:
    """Frozen OIDC discovery parameters for a single IdP."""

    issuer: str
    """Required ``iss`` claim value."""

    jwks_uri: str
    """JWKS URL for signature verification."""

    audience: str | Sequence[str]
    """Required ``aud`` value(s) — typically your OAuth client id."""

    algorithms: Sequence[str] = attrs.field(factory=lambda: ("RS256",))
    """Allowed JWS algorithms."""

    tenant_claim: str | None = attrs.field(default=None)
    """Optional claim mapped to ``issuer_tenant_hint``."""

    leeway: timedelta = attrs.field(default=timedelta(seconds=10))
    """Clock-skew leeway for JWT validation."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.leeway.total_seconds() <= 0:
            raise exc.configuration("Leeway must be positive")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableOidcIdpVerifier:
    """Build :class:`OidcTokenVerifier` from a frozen :class:`OidcIdpPreset`."""

    preset: OidcIdpPreset
    """Issuer, JWKS URI, and audience policy."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> TokenVerifierPort:
        _ = ctx, spec
        return OidcTokenVerifier(
            key_provider=JwksKeyProvider(jwks_uri=self.preset.jwks_uri),
            algorithms=tuple(self.preset.algorithms),
            audience=self.preset.audience,
            issuer=self.preset.issuer,
            enforce_issuer_and_audience=True,
            leeway=self.preset.leeway,
            claim_mapper=OidcClaimMapper(tenant_claim=self.preset.tenant_claim),
        )
