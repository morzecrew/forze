from datetime import UTC, datetime
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    TokenVerifierPort,
    VerifiedAssertion,
)
from forze.application.contracts.document import DocumentQueryPort
from forze.base.exceptions import exc

from ..domain.models.session import ReadSession
from ..services import AccessTokenClaims, AccessTokenService

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

    When ``session_qry`` is wired, the verifier requires a ``sid`` claim and rejects tokens
    whose session row is missing, revoked, or rotated — so logout and refresh rotation
    invalidate bearer access before JWT ``exp``. The session row must also match token
    ``sub`` (and ``tid`` when both token and session carry tenant metadata).
    """

    access_svc: AccessTokenService
    """Forze JWT access-token service."""

    session_qry: DocumentQueryPort[ReadSession] | None = None
    """When set, enforce session binding via the ``sid`` claim."""

    # ....................... #

    async def verify_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> VerifiedAssertion:
        claims = self.access_svc.verify_token(credentials.token)

        if self.session_qry is not None:
            await self._require_active_session(claims)

        issued_at = datetime.fromtimestamp(claims["iat"], tz=UTC)
        expires_at = datetime.fromtimestamp(claims["exp"], tz=UTC)

        return VerifiedAssertion(
            issuer=claims["iss"],
            subject=claims["sub"],
            audience=claims["aud"],
            issuer_tenant_hint=claims.get("tid"),
            issued_at=issued_at,
            expires_at=expires_at,
            claims=dict(claims),
        )

    # ....................... #

    async def _require_active_session(self, claims: AccessTokenClaims) -> None:
        sid_raw = claims.get("sid")
        if sid_raw is None:
            raise exc.authentication(
                "Invalid access token",
                code="invalid_access_token",
            )

        try:
            session_id = UUID(sid_raw)
        except ValueError as e:
            raise exc.authentication(
                "Invalid access token",
                code="invalid_access_token",
            ) from e

        session = await self.session_qry.find(  # type: ignore[union-attr]
            filters={
                "$values": {
                    "id": session_id,
                },
            },
        )

        if (
            session is None
            or session.revoked_at is not None
            or session.rotated_at is not None
        ):
            raise exc.authentication(
                "Session revoked",
                code="session_revoked",
            )

        try:
            token_principal_id = UUID(claims["sub"])
        except ValueError as e:
            raise exc.authentication(
                "Invalid access token",
                code="invalid_access_token",
            ) from e

        if session.principal_id != token_principal_id:
            raise exc.authentication(
                "Session does not match token subject",
                code="session_subject_mismatch",
            )

        tid_raw = claims.get("tid")
        if tid_raw is not None and session.tenant_id is not None:
            if str(session.tenant_id) != tid_raw:
                raise exc.authentication(
                    "Session does not match token tenant",
                    code="session_tenant_mismatch",
                )
