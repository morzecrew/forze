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

    require_nonce: bool = attrs.field(default=False)
    """When ``True``, reject an ``id_token`` with no ``nonce`` claim (presence-only).

    Off by default. Forwarded to :attr:`OidcTokenVerifier.require_nonce`; value binding
    to the per-request nonce stays the callback handler's job (see that field's docs)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.leeway.total_seconds() <= 0:
            raise exc.configuration("Leeway must be positive")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableOidcIdpVerifier:
    """Build :class:`OidcTokenVerifier` from a frozen :class:`OidcIdpPreset`.

    The verifier — and its :class:`JwksKeyProvider` — is built **once** at construction
    and reused for every call, so the JWKS cache (and its ``PyJWKClient``) spans requests.
    Rebuilding it per verification would discard the cache and turn every token-auth'd
    request into an outbound JWKS fetch (an amplifier against the IdP). Wire this factory
    once (the ``TokenVerifierPort`` provider); do not reconstruct it per request.
    """

    preset: OidcIdpPreset
    """Issuer, JWKS URI, and audience policy."""

    # ....................... #

    _verifier: OidcTokenVerifier = attrs.field(
        init=False,
        repr=False,
        default=attrs.Factory(lambda self: self._build_verifier(), takes_self=True),
    )
    """The verifier built once from :attr:`preset`, reused across calls."""

    # ....................... #

    def _build_verifier(  # pyright: ignore[reportUnusedFunction]
        self,
    ) -> OidcTokenVerifier:
        return OidcTokenVerifier(
            key_provider=JwksKeyProvider(jwks_uri=self.preset.jwks_uri),
            algorithms=tuple(self.preset.algorithms),
            audience=self.preset.audience,
            issuer=self.preset.issuer,
            enforce_issuer_and_audience=True,
            leeway=self.preset.leeway,
            require_nonce=self.preset.require_nonce,
            claim_mapper=OidcClaimMapper(tenant_claim=self.preset.tenant_claim),
        )

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> TokenVerifierPort:
        _ = ctx, spec
        return self._verifier
