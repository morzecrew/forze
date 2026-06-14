"""JWT signer backed by a Vault Transit signing key (BYOK for access tokens).

Implements the structural ``SignerPort`` used by the identity plane's access-token
service: the RSA private key lives in Vault and never leaves it — Forze only ever
sends the signing input and receives a signature. Verification is local (the
public key is fetched once and cached), and the public key is exposed as a JWK so
external services can validate tokens via JWKS.

Wire it via ``AuthnKernelConfig(access_token_signer=VaultTransitSigner(...))``.
Requires a Transit signing key of type ``rsa-2048`` (or larger). RSA/RS256 only —
the signature needs no marshaling; ECDSA support would require DER→raw conversion.
"""

from __future__ import annotations

import json
from typing import Any, cast, final

import attrs
from cryptography.hazmat.primitives import serialization
from jwt.algorithms import RSAAlgorithm

from ..kernel.client import VaultClientPort

# ----------------------- #


@final
@attrs.define(slots=True)
class VaultTransitSigner:
    """Access-token signer whose RSA private key stays in Vault Transit (RS256)."""

    client: VaultClientPort
    """Vault client (Transit mount configured on its config)."""

    key_name: str
    """Transit signing key name."""

    kid: str | None = None
    """Optional key id, surfaced in the public JWK for rotation."""

    algorithm: str = attrs.field(default="RS256", init=False)
    """JWS algorithm written to the token header."""

    _public_pem: str | None = attrs.field(default=None, init=False, repr=False)
    """Cached PEM public key (fetched once from Vault)."""

    # ....................... #

    async def _public_key_pem(self) -> str:
        if self._public_pem is None:
            self._public_pem = await self.client.transit_public_key(self.key_name)

        return self._public_pem

    # ....................... #

    async def sign(self, signing_input: bytes) -> bytes:
        return await self.client.transit_sign(self.key_name, signing_input)

    async def verification_key(self) -> Any:
        return serialization.load_pem_public_key((await self._public_key_pem()).encode())

    async def public_jwk(self) -> dict[str, Any]:
        public_key = serialization.load_pem_public_key(
            (await self._public_key_pem()).encode(),
        )
        jwk: dict[str, Any] = json.loads(
            RSAAlgorithm(RSAAlgorithm.SHA256).to_jwk(cast(Any, public_key))
        )
        jwk["use"] = "sig"
        jwk["alg"] = self.algorithm

        if self.kid is not None:
            jwk["kid"] = self.kid

        return jwk
