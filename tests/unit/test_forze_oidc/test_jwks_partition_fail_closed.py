"""OIDC verification fails CLOSED when the IdP's JWKS endpoint is unreachable (a partition).

The offline path (`StaticKeyProvider`) always resolves a key, so the verifier admits a valid token.
The production path (`JwksKeyProvider`) fetches keys from a `jwks_uri`; when that endpoint is
unreachable — a real network fault, exercised here against a genuinely closed local port — key
resolution must reject the token (`invalid_oidc_signing_key`), never admit it unverified. This pins
the fault path the happy-path key tests (which stub the JWKS client to return a key) never reach:
a partitioned IdP cannot let a token through.
"""

from __future__ import annotations

import socket
from datetime import timedelta
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from forze.application.contracts.authn import AccessTokenCredentials
from forze.base.exceptions import CoreException
from forze_identity.authn.services import (
    AccessTokenConfig,
    AccessTokenService,
    LocalAsymmetricSigner,
)
from forze_identity.oidc import OidcTokenVerifier, StaticKeyProvider
from forze_identity.oidc.keys import JwksKeyProvider

# ----------------------- #

_ISS = "https://issuer.example"
_AUD = "my-app"


def _closed_port() -> int:
    """Bind a socket to an ephemeral port, then close it — nothing listens there, so a connect
    attempt is refused immediately (a deterministic stand-in for an unreachable JWKS host)."""

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()

    return port


async def _rs256_token() -> tuple[str, rsa.RSAPrivateKey]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    signer = LocalAsymmetricSigner(private_key=private_key, algorithm="RS256", kid="k1")
    service = AccessTokenService(
        signer=signer, config=AccessTokenConfig(issuer=_ISS, audience=_AUD)
    )
    token = await service.issue_token(principal_id=uuid4())

    return token, private_key


def _verifier(key_provider: object) -> OidcTokenVerifier:
    return OidcTokenVerifier(
        key_provider=key_provider,  # type: ignore[arg-type]
        algorithms=("RS256",),
        issuer=_ISS,
        audience=_AUD,
    )


# ....................... #


class TestJwksPartitionFailsClosed:
    async def test_reachable_key_provider_admits_a_valid_token(self) -> None:
        # Reference: the key resolves (offline), so a well-formed token is accepted.
        token, private_key = await _rs256_token()
        verifier = _verifier(StaticKeyProvider(key=private_key.public_key()))

        assertion = await verifier.verify_token(AccessTokenCredentials(token=token))

        assert assertion.subject is not None

    async def test_unreachable_jwks_rejects_rather_than_admits(self) -> None:
        # Fault: the real JwksKeyProvider fetches from an unreachable endpoint. Key resolution fails,
        # and the verifier rejects (fail closed) — it does NOT fall open and admit the token.
        token, _ = await _rs256_token()
        dead_uri = f"http://127.0.0.1:{_closed_port()}/.well-known/jwks.json"
        verifier = _verifier(
            JwksKeyProvider(
                jwks_uri=dead_uri,
                timeout=timedelta(seconds=1),
                cache_ttl=timedelta(seconds=1),
            )
        )

        with pytest.raises(CoreException) as excinfo:
            await verifier.verify_token(AccessTokenCredentials(token=token))

        assert excinfo.value.code == "invalid_oidc_signing_key"
