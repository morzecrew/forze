import hashlib
import threading
from collections import OrderedDict
from collections.abc import Mapping
from datetime import timedelta
from math import isfinite
from typing import Any, Final, NotRequired, TypedDict
from uuid import UUID

import attrs
import jwt
import orjson
from cryptography.hazmat.primitives.asymmetric import dsa, ec, ed448, ed25519, rsa
from jwt.utils import base64url_encode

from forze.base.exceptions import exc
from forze.base.primitives import monotonic, utcnow

from .signing import SignerPort

# ----------------------- #

_MAX_VERIFY_CACHE_TTL: Final[timedelta] = timedelta(minutes=5)
"""The longest a verified token's signature and claims may be reused, as for the OIDC caches."""

_Verified = tuple[float, str | None, tuple[Any, ...], dict[str, Any]]
"""A remembered verification: its monotonic deadline, the header ``kid``, what ``jwt.decode``
read besides the token (key, algorithm, issuer, audience) and the claims it returned."""

_IMMUTABLE_KEY: Final = (
    str,
    bytes,
    rsa.RSAPublicKey,
    ec.EllipticCurvePublicKey,
    ed25519.Ed25519PublicKey,
    ed448.Ed448PublicKey,
    dsa.DSAPublicKey,
)
"""Verification key types a cached verification may be compared against: a key that can change
in place (a ``PyJWK``, a ``bytearray``) would still equal itself after the change."""

_FLAT_CLAIM: Final = (str, int, float, type(None))
"""Claim value types a cached verification may hold; a tuple, as a union costs more per check."""

# ....................... #


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

    verify_cache_ttl: timedelta = timedelta(seconds=30)
    """How long a token's verified signature and claims are reused for that same token; at
    most five minutes, zero to disable. A reuse still needs the same key, algorithm, issuer and
    audience, and still checks ``exp``/``iat``/``nbf``; session and revocation checks after it
    run on every request. A verification key that can change in place (a ``PyJWK``) is never
    cached."""

    verify_cache_max_entries: int = 1024
    """Verified tokens kept; the least recently used goes first."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.expires_in.total_seconds() <= 0:
            raise exc.configuration("Expires in must be positive")

        if (
            not isinstance(self.verify_cache_ttl, timedelta)
            or not timedelta(0) <= self.verify_cache_ttl <= _MAX_VERIFY_CACHE_TTL
        ):
            raise exc.configuration(
                f"Verify cache TTL must be a timedelta between 0 and {_MAX_VERIFY_CACHE_TTL}",
            )

        if (
            not isinstance(self.verify_cache_max_entries, int)
            or isinstance(self.verify_cache_max_entries, bool)
            or self.verify_cache_max_entries < 1
        ):
            raise exc.configuration("Verify cache max entries must be an integer of at least 1")


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
    fam: NotRequired[str]
    """The session's family id; issued only with revocation states configured."""
    pep: NotRequired[int]
    """The principal's epoch at issue; issued only with revocation states configured."""


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

    # SHA-256 of the token -> its last verification, least recently used first: see ``_decode``.
    _verified: OrderedDict[bytes, _Verified] = attrs.field(
        factory=OrderedDict[bytes, _Verified],
        init=False,
        repr=False,
    )
    _verified_lock: threading.Lock = attrs.field(factory=threading.Lock, init=False, repr=False)

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
        family_id: UUID | None = None,
        epoch: int | None = None,
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

        if family_id is not None:
            payload["fam"] = str(family_id)

        if epoch is not None:
            payload["pep"] = epoch

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

    async def _decode(self, token: str, *, leeway: timedelta) -> dict[str, Any]:
        """Verify *token*'s signature and claims, reusing a recent verification of it.

        ``jwt.decode`` here reads no clock, so its verdict depends only on the token, the key,
        the algorithm, the issuer and the audience. An entry, keyed by the token's SHA-256, is
        reused only while the token's ``kid`` still selects a verifier whose key and algorithm,
        with the configured issuer and audience, equal those it was verified with, and for at
        most :attr:`AccessTokenConfig.verify_cache_ttl` after the verification began. The time
        claims are checked on every call, and only a token that passed every check is stored.
        """

        ttl = self.config.verify_cache_ttl.total_seconds()
        began = monotonic()
        digest = hashlib.sha256(token.encode()).digest() if ttl else b""
        hit = self._recall(digest, now=began) if ttl else None

        kid = hit[1] if hit is not None else jwt.get_unverified_header(token).get("kid")
        verifier = self._verifier_for(kid)
        key = await verifier.verification_key()
        inputs = (key, verifier.algorithm, self.config.issuer, self.config.audience)

        # The deadline again: ``verification_key()`` may have awaited past it.
        if hit is not None and hit[2] == inputs and monotonic() < hit[0]:
            res = hit[3]

        else:
            hit = None
            res = jwt.decode(  # pyright: ignore[reportUnknownMemberType]
                jwt=token,
                key=key,
                algorithms=[verifier.algorithm],
                issuer=self.config.issuer,
                audience=self.config.audience,
                # The times are checked below, on the clock that stamped them: PyJWT would
                # check them on the wall clock, which a bound time source does not move.
                options={
                    "require": ["exp", "iat", "sub", "iss", "aud"],
                    "verify_exp": False,
                    "verify_iat": False,
                    "verify_nbf": False,
                },
            )

        _check_times(res, now=utcnow().timestamp(), leeway=leeway.total_seconds())

        # Flat claims only: the shallow copy a caller gets then shares nothing it could mutate.
        if (
            ttl
            and hit is None
            and isinstance(key, _IMMUTABLE_KEY)
            and all(isinstance(v, _FLAT_CLAIM) for v in res.values())
        ):
            self._remember(digest, (began + ttl, kid, inputs, res))

        return res

    # ....................... #

    def _recall(self, digest: bytes, *, now: float) -> _Verified | None:
        with self._verified_lock:
            entry = self._verified.get(digest)

            if entry is None:
                return None

            if now >= entry[0]:
                del self._verified[digest]
                return None

            self._verified.move_to_end(digest)

            return entry

    # ....................... #

    def _remember(self, digest: bytes, entry: _Verified) -> None:
        with self._verified_lock:
            self._verified[digest] = entry
            self._verified.move_to_end(digest)

            while len(self._verified) > self.config.verify_cache_max_entries:
                self._verified.popitem(last=False)

    # ....................... #

    async def verify_token(
        self,
        token: str,
        *,
        leeway: timedelta = timedelta(seconds=10),
    ) -> AccessTokenClaims:
        try:
            res = await self._decode(token, leeway=leeway)

            self._n_verified += 1
            # A copy: the claims a cache entry holds are never handed out.
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


# ....................... #


def _check_times(claims: Mapping[str, Any], *, now: float, leeway: float) -> None:
    """Judge ``exp``, ``iat`` and ``nbf`` against *now*, as PyJWT judges them against its own
    clock: expired at ``exp`` less the leeway, not yet valid before ``iat`` or ``nbf`` plus it.

    :raises jwt.InvalidTokenError: a time claim that is not a finite number, or not yet valid.
    :raises jwt.ExpiredSignatureError: the token has expired.
    """

    times = {claim: claims[claim] for claim in ("exp", "iat", "nbf") if claim in claims}

    for claim, value in times.items():
        # NaN and infinity are floats, not times: an ``exp`` of either would never expire.
        if isinstance(value, bool) or not isinstance(value, int | float) or not isfinite(value):
            raise jwt.InvalidTokenError(f"The {claim} claim must be a finite number")

    if times["exp"] <= now - leeway:
        raise jwt.ExpiredSignatureError("Signature has expired")

    if any(times[claim] > now + leeway for claim in ("iat", "nbf") if claim in times):
        raise jwt.ImmatureSignatureError("The token is not yet valid")
