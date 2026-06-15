from datetime import timedelta
from typing import NotRequired, TypedDict
from uuid import UUID

import attrs
import jwt
import orjson
from jwt.utils import base64url_encode

from forze.base.exceptions import exc
from forze.base.primitives import utcnow

from .signing import SignerPort

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AccessTokenConfig:
    """Access token configuration.

    The JWS algorithm is determined by the wired :class:`SignerPort`, not here.
    """

    issuer: str = "forze"
    """Issuer of the token."""

    audience: str = "forze"
    """Audience of the token."""

    expires_in: timedelta = timedelta(minutes=15)
    """Expiration time of the token."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.expires_in.total_seconds() <= 0:
            raise exc.configuration("Expires in must be positive")


# ....................... #


class AccessTokenClaims(TypedDict):
    """JWT payload for access tokens."""

    iss: str
    aud: str
    sub: str
    iat: int
    exp: int
    tid: NotRequired[str]
    sid: NotRequired[str]
    """Session id binding the access token to a refresh session row."""


# ....................... #


@attrs.define(slots=True, kw_only=True)
class AccessTokenService:
    """Access token service.

    Signs/verifies via a pluggable :class:`SignerPort`, so the private key may be
    an in-process secret (HS256), an in-process asymmetric key, or a KMS-held key
    that never leaves the backend (BYOK).
    """

    signer: SignerPort
    """Signer used to **issue** tokens."""

    config: AccessTokenConfig = attrs.field(factory=AccessTokenConfig)

    additional_verifiers: tuple[SignerPort, ...] = attrs.field(factory=tuple)
    """Extra signers whose keys are also **accepted on verify**, selected by the
    token's ``kid``. Holds the previous key during a rotation overlap so in-flight
    tokens keep verifying until they expire; drop it once they have."""

    # ....................... #

    async def issue_token(
        self,
        *,
        principal_id: UUID,
        tenant_id: UUID | None = None,
        session_id: UUID | None = None,
    ) -> str:
        now = utcnow()
        exp = now + self.config.expires_in

        payload: dict[str, object] = {
            "iss": self.config.issuer,
            "aud": self.config.audience,
            "sub": str(principal_id),
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
        }

        if tenant_id is not None:
            payload["tid"] = str(tenant_id)

        if session_id is not None:
            payload["sid"] = str(session_id)

        header: dict[str, object] = {"alg": self.signer.algorithm, "typ": "JWT"}

        if self.signer.kid is not None:
            header["kid"] = self.signer.kid

        signing_input = (
            base64url_encode(orjson.dumps(header))
            + b"."
            + base64url_encode(orjson.dumps(payload))
        )
        signature = await self.signer.sign(signing_input)

        return (signing_input + b"." + base64url_encode(signature)).decode("ascii")

    # ....................... #

    def _verifier_for(self, kid: str | None) -> SignerPort:
        """Select the signer to verify with, by the token header's ``kid``.

        No ``kid`` (symmetric / legacy) → the issuing signer. A ``kid`` that
        matches no configured key is rejected as an invalid token.
        """

        if kid is None:
            return self.signer

        for candidate in (self.signer, *self.additional_verifiers):
            if candidate.kid == kid:
                return candidate

        raise exc.authentication("Unknown signing key", code="invalid_access_token")

    # ....................... #

    async def verify_token(
        self,
        token: str,
        *,
        leeway: timedelta = timedelta(seconds=10),
    ) -> AccessTokenClaims:
        try:
            kid = jwt.get_unverified_header(token).get("kid")
            verifier = self._verifier_for(kid)

            res = jwt.decode(  # pyright: ignore[reportUnknownMemberType]
                jwt=token,
                key=await verifier.verification_key(),
                algorithms=[verifier.algorithm],
                issuer=self.config.issuer,
                audience=self.config.audience,
                options={
                    "require": ["exp", "iat", "sub", "iss", "aud"],
                },
                leeway=leeway,
            )

            return AccessTokenClaims(**res)  # type: ignore[typeddict-item]

        except jwt.ExpiredSignatureError:
            raise exc.authentication(
                "Access token expired",
                code="access_token_expired",
            )

        except jwt.InvalidTokenError:
            raise exc.authentication(
                "Invalid access token",
                code="invalid_access_token",
            )
