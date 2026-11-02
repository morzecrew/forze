"""OIDC IdP preset configuration and verifier factory."""

from collections.abc import Sequence
from datetime import timedelta
from typing import Any, Self, final

import attrs
import httpx

from forze.application.contracts.authn import AuthnSpec, TokenVerifierPort
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from .claims import OidcClaimMapper
from .discovery import OidcProviderMetadata, fetch_oidc_configuration
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

    claim_mapper: OidcClaimMapper | None = attrs.field(default=None)
    """Claim mapper for the verifier, for ``email`` / ``roles`` / ``groups`` extraction. When
    set, put the tenant claim on it rather than on :attr:`tenant_claim`."""

    metadata: OidcProviderMetadata | None = attrs.field(default=None)
    """The discovery document this preset was built from (see :meth:`from_issuer`), kept for
    the introspection and token endpoints it advertises."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.leeway.total_seconds() <= 0:
            raise exc.configuration("Leeway must be positive")

        if self.claim_mapper is not None and self.tenant_claim is not None:
            raise exc.configuration(
                "Set tenant_claim on the claim_mapper, not on the preset: with both set, "
                "one would be silently ignored",
            )

        if self.metadata is not None and self.metadata.issuer != self.issuer:
            raise exc.configuration("Preset metadata belongs to a different issuer")

    # ....................... #

    @classmethod
    async def from_issuer(
        cls,
        issuer: str,
        *,
        audience: str | Sequence[str],
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
        **overrides: Any,
    ) -> Self:
        """Build a preset from the issuer's discovery document.

        A construction-time call — run it at startup, never per request. Fills
        :attr:`jwks_uri` and :attr:`metadata`; the remaining fields come from *overrides*
        (``algorithms``, ``claim_mapper``, ``leeway``, …). See
        :func:`~forze_identity.oidc.fetch_oidc_configuration` for what is validated.

        :param issuer: The issuer URL exactly as tokens carry it in ``iss``.
        :param audience: Required ``aud`` value(s).
        :param timeout: Discovery request timeout in seconds.
        :param transport: Optional httpx transport (inject a mock in tests).
        :param overrides: Further preset fields.
        :returns: The preset.
        :raises CoreException: When discovery fails or the document is refused.
        """

        metadata = await fetch_oidc_configuration(issuer, timeout=timeout, transport=transport)

        return cls(
            issuer=metadata.issuer,
            jwks_uri=metadata.jwks_uri,
            audience=audience,
            metadata=metadata,
            **overrides,
        )


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
            claim_mapper=(
                self.preset.claim_mapper
                if self.preset.claim_mapper is not None
                else OidcClaimMapper(tenant_claim=self.preset.tenant_claim)
            ),
        )

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> TokenVerifierPort:
        _ = ctx, spec
        return self._verifier
