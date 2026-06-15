"""JWT signer backed by a Vault Transit signing key (BYOK for access tokens).

Implements the structural ``SignerPort`` used by the identity plane's access-token
service: the private key lives in Vault and never leaves it — Forze only ever sends
the signing input and receives a signature. Verification is local (the public key is
fetched once and cached), and the public key is exposed as a JWK so external services
can validate tokens via JWKS.

Wire it via ``AuthnKernelConfig(access_token_signer=VaultTransitSigner(...))``.
Supports two algorithms, matched to the Transit key type:

- ``RS256`` (default) — a ``rsa-2048`` (or larger) key; Vault signs ``pkcs1v15``.
- ``ES256`` — an ``ecdsa-p256`` key; Vault signs with ``marshaling_algorithm="jws"``
  so it returns the raw ``r||s`` an ES256 JWS expects (no DER→raw conversion here).
"""

import json
from typing import Any, cast, final

import attrs
from cryptography.hazmat.primitives import serialization
from jwt.algorithms import ECAlgorithm, RSAAlgorithm

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from ..kernel.client import VaultClientPort

# ----------------------- #

_SUPPORTED_ALGORITHMS = frozenset({"RS256", "ES256"})

# ....................... #


@final
@attrs.define(slots=True)
class VaultTransitSigner:
    """Access-token signer whose private key stays in Vault Transit (RS256 or ES256)."""

    client: VaultClientPort
    """Vault client (Transit mount configured on its config)."""

    key_name: str
    """Transit signing key name (``rsa-*`` for RS256, ``ecdsa-p256`` for ES256)."""

    kid: str | None = None
    """Optional key id, surfaced in the public JWK for rotation."""

    algorithm: str = "RS256"  #! probably enforce Literal
    """JWS algorithm written to the token header: ``RS256`` or ``ES256``."""

    _public_pem: str | None = attrs.field(default=None, init=False, repr=False)
    """Cached PEM public key (fetched once from Vault)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.algorithm not in _SUPPORTED_ALGORITHMS:
            raise exc.configuration(
                f"VaultTransitSigner supports {sorted(_SUPPORTED_ALGORITHMS)}, "
                f"got {self.algorithm!r}.",
            )

    # ....................... #

    @property
    def _is_ecdsa(self) -> bool:
        return self.algorithm == "ES256"

    # ....................... #

    async def _public_key_pem(self) -> str:
        if self._public_pem is None:
            self._public_pem = await self.client.transit_public_key(self.key_name)

        return self._public_pem

    # ....................... #

    async def sign(self, signing_input: bytes) -> bytes:
        if self._is_ecdsa:
            return await self.client.transit_sign(
                self.key_name,
                signing_input,
                signature_algorithm=None,
                marshaling_algorithm="jws",
            )

        return await self.client.transit_sign(self.key_name, signing_input)

    # ....................... #

    async def verification_key(self) -> Any:
        return serialization.load_pem_public_key(
            (await self._public_key_pem()).encode()
        )

    # ....................... #

    async def public_jwk(self) -> JsonDict:
        public_key = serialization.load_pem_public_key(
            (await self._public_key_pem()).encode(),
        )
        algo = (
            ECAlgorithm(ECAlgorithm.SHA256)
            if self._is_ecdsa
            else RSAAlgorithm(RSAAlgorithm.SHA256)
        )
        jwk: JsonDict = json.loads(algo.to_jwk(cast(Any, public_key)))
        jwk["use"] = "sig"
        jwk["alg"] = self.algorithm

        if self.kid is not None:
            jwk["kid"] = self.kid

        return jwk
