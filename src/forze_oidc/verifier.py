from forze_oidc._compat import require_oidc

require_oidc()

# ....................... #

from collections.abc import Sequence
from datetime import timedelta
from typing import final

import attrs
from jwt import ExpiredSignatureError, InvalidTokenError
from jwt import decode as jwt_decode

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    TokenVerifierPort,
    VerifiedAssertion,
)
from forze.base.errors import AuthenticationError

from .claims import OidcClaimMapper
from .keys import SigningKeyProviderPort

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OidcTokenVerifier(TokenVerifierPort):
    """Verify an OIDC-style JWT (RS256/ES256/HS256) and emit a :class:`VerifiedAssertion`.

    Pluggable signing-key resolution (:class:`SigningKeyProviderPort`) keeps the verifier
    testable without network access. Pluggable :class:`OidcClaimMapper` lets each IdP map
    its idiosyncratic tenant/audience claims onto the canonical assertion shape without
    branching here.

    Pair this verifier with :class:`forze_authn.MappingTableResolver` to map external
    subjects to internal Forze :class:`UUID`-shaped principals (good for SSO with admin
    overrides), or with :class:`forze_authn.DeterministicUuidResolver` for stateless
    deterministic mapping.
    """

    key_provider: SigningKeyProviderPort
    """Provider that returns the verification key for an inbound token."""

    algorithms: Sequence[str] = attrs.field(factory=lambda: ("RS256",))
    """Allowed JWS algorithms; rejects anything outside this allowlist."""

    audience: str | Sequence[str] | None = attrs.field(default=None)
    """Required ``aud`` value(s); when ``None``, audience is not enforced."""

    issuer: str | None = attrs.field(default=None)
    """Required ``iss`` value; when ``None``, issuer is not enforced."""

    leeway: timedelta = attrs.field(default=timedelta(seconds=10))
    """Clock-skew leeway for ``iat``/``exp``/``nbf`` validation."""

    claim_mapper: OidcClaimMapper = attrs.field(factory=OidcClaimMapper)
    """Maps the verified claim payload onto the canonical assertion shape."""

    # ....................... #

    async def verify_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> VerifiedAssertion:
        try:
            key = self.key_provider.get_signing_key(credentials.token)

            claims = jwt_decode(  # pyright: ignore[reportUnknownMemberType]
                jwt=credentials.token,
                key=key,
                algorithms=list(self.algorithms),
                audience=self.audience,
                issuer=self.issuer,
                leeway=self.leeway,
                options={"require": ["iss", "sub", "exp"]},
            )

        except ExpiredSignatureError as exc:
            raise AuthenticationError(
                "OIDC token expired",
                code="oidc_token_expired",
            ) from exc

        except InvalidTokenError as exc:
            raise AuthenticationError(
                "Invalid OIDC token",
                code="invalid_oidc_token",
            ) from exc

        return self.claim_mapper.map(claims)
