from datetime import UTC, datetime
from typing import final

import attrs

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    TokenVerifierPort,
    VerifiedAssertion,
)

from ..services import AccessTokenService

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ForzeJwtTokenVerifier(TokenVerifierPort):
    """Verify a first-party Forze access JWT and emit a :class:`VerifiedAssertion`.

    Treats ``AccessTokenCredentials.scheme`` as a routing hint only; the underlying
    :class:`AccessTokenService` is the security gate (signature, expiry, issuer, audience).
    The emitted ``issuer`` is taken from the verified ``iss`` claim so that resolvers can
    distinguish first-party Forze tokens from any other JWT-shaped issuer wired on the
    same route.
    """

    access_svc: AccessTokenService
    """Forze JWT access-token service."""

    # ....................... #

    async def verify_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> VerifiedAssertion:
        claims = self.access_svc.verify_token(credentials.token)

        issued_at = datetime.fromtimestamp(claims["iat"], tz=UTC)
        expires_at = datetime.fromtimestamp(claims["exp"], tz=UTC)

        return VerifiedAssertion(
            issuer=claims["iss"],
            subject=claims["sub"],
            audience=claims["aud"],
            tenant_hint=claims.get("tid"),
            issued_at=issued_at,
            expires_at=expires_at,
            claims=dict(claims),
        )
