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


@attrs.define(slots=True, frozen=True, kw_only=True)
class SigningStats:
    """Cumulative, monotonic token sign/verify counters for an access-token service.

    A snapshot taken by :meth:`AccessTokenService.signing_stats`, sampled by the
    OpenTelemetry observable instruments in ``instrument_signing``. Carries the issuing
    signer's :attr:`algorithm`/:attr:`kid` so a dashboard can attribute counts to a key.
    """

    algorithm: str
    """JWS algorithm of the issuing signer (e.g. ``RS256``)."""

    kid: str | None
    """Key id of the issuing signer, when set (asymmetric/rotated keys)."""

    signed: int
    """Access tokens signed (``issue_token``)."""

    verified: int
    """Access tokens that verified successfully (``verify_token``)."""

    verify_failed: int
    """Verifications rejected (expired or invalid token)."""


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

    # Cumulative observability counters (sampled by ``instrument_signing``).
    _n_signed: int = attrs.field(default=0, init=False)
    _n_verified: int = attrs.field(default=0, init=False)
    _n_verify_failed: int = attrs.field(default=0, init=False)

    # ``kid`` -> verifier, resolved once so per-request verify is an O(1) lookup
    # rather than a linear scan that also rebuilds the candidate tuple each call.
    _verifier_by_kid: dict[str, SignerPort] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # First match wins (the issuing signer takes precedence over rotation
        # overlap keys), matching the prior in-order scan.
        for candidate in (self.signer, *self.additional_verifiers):
            if candidate.kid is not None and candidate.kid not in self._verifier_by_kid:
                self._verifier_by_kid[candidate.kid] = candidate

    # ....................... #

    def signing_stats(self) -> SigningStats:
        """Snapshot the cumulative sign/verify counters for metrics export."""

        return SigningStats(
            algorithm=self.signer.algorithm,
            kid=self.signer.kid,
            signed=self._n_signed,
            verified=self._n_verified,
            verify_failed=self._n_verify_failed,
        )

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
            base64url_encode(orjson.dumps(header)) + b"." + base64url_encode(orjson.dumps(payload))
        )
        signature = await self.signer.sign(signing_input)
        self._n_signed += 1

        return (signing_input + b"." + base64url_encode(signature)).decode("ascii")

    # ....................... #

    def _verifier_for(self, kid: str | None) -> SignerPort:
        """Select the signer to verify with, by the token header's ``kid``.

        No ``kid`` (symmetric / legacy) → the issuing signer. A ``kid`` that
        matches no configured key is rejected as an invalid token.
        """

        if kid is None:
            return self.signer

        verifier = self._verifier_by_kid.get(kid)

        if verifier is None:
            raise exc.authentication("Unknown signing key", code="invalid_access_token")

        return verifier

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

            self._n_verified += 1
            return AccessTokenClaims(**res)  # type: ignore[typeddict-item]

        except jwt.ExpiredSignatureError as err:
            self._n_verify_failed += 1
            raise exc.authentication(
                "Access token expired",
                code="access_token_expired",
            ) from err

        except jwt.InvalidTokenError as err:
            self._n_verify_failed += 1
            raise exc.authentication(
                "Invalid access token",
                code="invalid_access_token",
            ) from err
