"""Pluggable JWT signers — symmetric (HS256) and asymmetric (RS256/ES256).

The :class:`SignerPort` abstracts how an access token's ``header.payload`` signing
input is signed and verified, so the private key can live anywhere: an in-process
secret (:class:`Hs256Signer`, the default — behavior-preserving), an in-process
asymmetric key (:class:`LocalAsymmetricSigner`), or a KMS that never exposes the
private key (a BYOK signer implemented in an integration package, e.g. Vault
Transit). ``sign`` is async so a KMS round-trip fits; ``verification_key`` is async
so a KMS-held public key can be fetched once and cached.

Asymmetric signers expose a public JWK so verifiers (including external services)
can validate tokens via a JWKS document (:func:`jwks_document`).
"""

import json
from collections.abc import Awaitable
from typing import Any, Protocol, final

import attrs
from jwt.algorithms import ECAlgorithm, HMACAlgorithm, RSAAlgorithm

from forze.base.exceptions import exc

# ----------------------- #


class SignerPort(Protocol):
    """Signs and verifies a JWT signing input; exposes its public JWK when asymmetric."""

    @property
    def algorithm(self) -> str:
        """JWS algorithm (e.g. ``HS256``, ``RS256``, ``ES256``) written to the header."""

        ...  # pragma: no cover

    @property
    def kid(self) -> str | None:
        """Key id for rotation, written to the token header and the public JWK.

        ``None`` for symmetric signers (no published key, no rotation overlap).
        """

        ...  # pragma: no cover

    def sign(self, signing_input: bytes) -> Awaitable[bytes]:
        """Return the raw JWS signature for ``base64url(header).base64url(payload)``."""

        ...  # pragma: no cover

    def verification_key(self) -> Awaitable[Any]:
        """Return the key for verification — the secret (symmetric) or public key."""

        ...  # pragma: no cover

    def public_jwk(self) -> Awaitable[dict[str, Any] | None]:
        """Return the public key as a JWK, or ``None`` for symmetric signers."""

        ...  # pragma: no cover


# ....................... #


def _asymmetric_algorithm(algorithm: str) -> RSAAlgorithm | ECAlgorithm:
    digest = f"SHA{algorithm[2:]}"  # RS256 -> SHA256, ES384 -> SHA384, ...

    if algorithm.startswith("RS"):
        return RSAAlgorithm(getattr(RSAAlgorithm, digest))

    if algorithm.startswith("ES"):
        return ECAlgorithm(getattr(ECAlgorithm, digest))

    raise exc.configuration(f"Unsupported asymmetric JWT algorithm: {algorithm!r}")


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Hs256Signer:
    """HMAC-SHA256 signer backed by an in-process secret (the default)."""

    secret: bytes = attrs.field(repr=False, validator=attrs.validators.min_len(32))

    @property
    def algorithm(self) -> str:
        return "HS256"

    @property
    def kid(self) -> str | None:
        return None

    async def sign(self, signing_input: bytes) -> bytes:
        return HMACAlgorithm(HMACAlgorithm.SHA256).sign(signing_input, self.secret)

    async def verification_key(self) -> Any:
        return self.secret

    async def public_jwk(self) -> dict[str, Any] | None:
        return None


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LocalAsymmetricSigner:
    """Asymmetric signer using an in-process private key (RS256/ES256).

    The key is held by the process — for a BYOK posture where the private key
    never leaves a KMS, implement :class:`SignerPort` against that KMS instead.
    This signer still demonstrates the asymmetric + JWKS verification path.
    """

    private_key: Any = attrs.field(repr=False)
    """A ``cryptography`` private key (RSA or EC)."""

    algorithm: str = "RS256"
    """``RS256``/``RS384``/``RS512`` or ``ES256``/``ES384``/``ES512``."""

    kid: str | None = None
    """Optional key id, surfaced in the public JWK for key rotation."""

    async def sign(self, signing_input: bytes) -> bytes:
        return _asymmetric_algorithm(self.algorithm).sign(signing_input, self.private_key)

    async def verification_key(self) -> Any:
        return self.private_key.public_key()

    async def public_jwk(self) -> dict[str, Any]:
        jwk: dict[str, Any] = json.loads(
            _asymmetric_algorithm(self.algorithm).to_jwk(self.private_key.public_key())
        )
        jwk["use"] = "sig"
        jwk["alg"] = self.algorithm

        if self.kid is not None:
            jwk["kid"] = self.kid

        return jwk


# ....................... #


async def jwks_document(*signers: SignerPort) -> dict[str, Any]:
    """Build a JWKS (``{"keys": [...]}``) from the asymmetric *signers*.

    Symmetric signers contribute nothing (their key must stay secret). Serve the
    result at a ``jwks_uri`` so verifiers can validate RS256/ES256 tokens.
    """

    keys = [jwk for signer in signers if (jwk := await signer.public_jwk()) is not None]
    return {"keys": keys}
