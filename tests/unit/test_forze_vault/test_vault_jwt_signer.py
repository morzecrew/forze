"""Unit tests for the Vault Transit JWT signer (real RSA key, mocked Vault transport)."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("jwt")

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from forze.base.exceptions import CoreException
from forze_identity.authn.services import AccessTokenService, jwks_document
from forze_vault import VaultTransitSigner
from forze_vault.kernel.client import VaultClientPort

# ----------------------- #


def _signer_with_real_key(kid: str | None = "v1") -> tuple[VaultTransitSigner, MagicMock]:
    """A VaultTransitSigner whose mocked client signs with a real in-test RSA key."""

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = (
        private_key.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode("ascii")
    )

    async def _sign(_name: str, data: bytes) -> bytes:
        return private_key.sign(data, padding.PKCS1v15(), hashes.SHA256())

    client = MagicMock(spec=VaultClientPort)
    client.transit_sign = AsyncMock(side_effect=_sign)
    client.transit_public_key = AsyncMock(return_value=pem)

    return VaultTransitSigner(client=client, key_name="jwt", kid=kid), client


# ....................... #


async def test_issue_and_verify_round_trip() -> None:
    signer, _client = _signer_with_real_key()
    svc = AccessTokenService(signer=signer)
    pid = uuid4()

    claims = await svc.verify_token(await svc.issue_token(principal_id=pid))

    assert claims["sub"] == str(pid)


async def test_public_key_is_fetched_once_and_cached() -> None:
    signer, client = _signer_with_real_key()
    svc = AccessTokenService(signer=signer)

    token = await svc.issue_token(principal_id=uuid4())
    await svc.verify_token(token)
    await signer.public_jwk()

    assert client.transit_public_key.await_count == 1


async def test_public_jwk_shape() -> None:
    signer, _client = _signer_with_real_key(kid="key-9")

    doc = await jwks_document(signer)

    assert doc["keys"][0]["kty"] == "RSA"
    assert doc["keys"][0]["kid"] == "key-9"
    assert doc["keys"][0]["use"] == "sig"
    assert doc["keys"][0]["alg"] == "RS256"


async def test_tampered_token_rejected() -> None:
    signer, _client = _signer_with_real_key()
    svc = AccessTokenService(signer=signer)
    token = await svc.issue_token(principal_id=uuid4())

    head, payload, sig = token.split(".")
    tampered = f"{head}.{payload}.{sig[:-2]}xx"

    with pytest.raises(CoreException):
        await svc.verify_token(tampered)
