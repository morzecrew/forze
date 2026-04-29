from datetime import timedelta
from typing import TypedDict
from uuid import UUID

import attrs
import jwt

from forze.base.errors import AuthenticationError
from forze.base.primitives import utcnow

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AccessTokenConfig:
    """Access token configuration."""

    issuer: str = "forze"
    """Issuer of the token."""

    audience: str = "forze"
    """Audience of the token."""

    expires_in: timedelta = timedelta(minutes=15)
    """Expiration time of the token."""

    algorithm: str = "HS256"
    """Algorithm of the token."""


# ....................... #


class AccessTokenClaims(TypedDict):
    iss: str
    aud: str
    sub: str
    iat: int
    exp: int


# ....................... #


@attrs.define(slots=True, kw_only=True)
class AccessTokenService:
    """Access token service."""

    secret_key: bytes = attrs.field(validator=attrs.validators.min_len(32))
    config: AccessTokenConfig = attrs.field(factory=AccessTokenConfig)

    # ....................... #

    def issue_token(self, *, principal_id: UUID) -> str:
        now = utcnow()
        exp = now + self.config.expires_in

        payload = AccessTokenClaims(
            iss=self.config.issuer,
            aud=self.config.audience,
            sub=str(principal_id),
            iat=int(now.timestamp()),
            exp=int(exp.timestamp()),
        )

        return jwt.encode(  # pyright: ignore[reportUnknownMemberType]
            payload=dict(payload),
            key=self.secret_key,
            algorithm=self.config.algorithm,
        )

    # ....................... #

    def verify_token(
        self,
        token: str,
        *,
        leeway: timedelta = timedelta(seconds=10),
    ) -> AccessTokenClaims:
        try:
            res = jwt.decode(  # pyright: ignore[reportUnknownMemberType]
                jwt=token,
                key=self.secret_key,
                algorithms=[self.config.algorithm],
                issuer=self.config.issuer,
                audience=self.config.audience,
                options={
                    "require": [
                        "exp",
                        "iat",
                        "sub",
                        "iss",
                        "aud",
                    ],
                },
                leeway=leeway,
            )

            return AccessTokenClaims(**res)  # type: ignore[typeddict-item]

        except jwt.ExpiredSignatureError:
            raise AuthenticationError(
                "Access token expired",
                code="access_token_expired",
            )

        except jwt.InvalidTokenError:
            raise AuthenticationError(
                "Invalid access token",
                code="invalid_access_token",
            )

    # ....................... #

    def try_decode_token(
        self,
        token: str,
        *,
        leeway: timedelta = timedelta(seconds=10),
    ) -> AccessTokenClaims | None:
        try:
            res = jwt.decode(  # pyright: ignore[reportUnknownMemberType]
                jwt=token,
                key=self.secret_key,
                algorithms=[self.config.algorithm],
                issuer=self.config.issuer,
                audience=self.config.audience,
                options={
                    "verify_signature": True,
                    "verify_exp": False,
                    "verify_iat": False,
                    "verify_nbf": False,
                    "verify_iss": False,
                    "verify_aud": False,
                    "require": [],
                },
                leeway=leeway,
            )

            return AccessTokenClaims(**res)  # type: ignore[typeddict-item]

        except Exception:
            return None
