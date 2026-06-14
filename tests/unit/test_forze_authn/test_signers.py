"""Tests for pluggable JWT signers (HS256 / asymmetric) and JWKS production."""

from __future__ import annotations

import secrets
from uuid import uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from jwt import PyJWK

from forze.base.exceptions import CoreException
from forze_identity.authn.services import (
    AccessTokenConfig,
    AccessTokenService,
    Hs256Signer,
    LocalAsymmetricSigner,
    jwks_document,
)

# ----------------------- #

_CFG = AccessTokenConfig(issuer="it", audience="api")


def _svc(signer) -> AccessTokenService:  # type: ignore[no-untyped-def]
    return AccessTokenService(signer=signer, config=_CFG)


# ----------------------- #


async def test_hs256_round_trip() -> None:
    svc = _svc(Hs256Signer(secret=secrets.token_bytes(32)))
    pid = uuid4()

    claims = await svc.verify_token(await svc.issue_token(principal_id=pid))

    assert claims["sub"] == str(pid)


@pytest.mark.parametrize(
    ("signer_factory", "alg"),
    [
        (lambda: rsa.generate_private_key(public_exponent=65537, key_size=2048), "RS256"),
        (lambda: ec.generate_private_key(ec.SECP256R1()), "ES256"),
    ],
)
async def test_asymmetric_round_trip(signer_factory, alg) -> None:  # type: ignore[no-untyped-def]
    svc = _svc(LocalAsymmetricSigner(private_key=signer_factory(), algorithm=alg, kid="k1"))
    pid = uuid4()

    token = await svc.issue_token(principal_id=pid, tenant_id=uuid4())
    claims = await svc.verify_token(token)

    assert claims["sub"] == str(pid)
    assert "tid" in claims


async def test_asymmetric_token_verifiable_via_jwks() -> None:
    """An external verifier validates our RS256 token using only the JWKS."""

    signer = LocalAsymmetricSigner(
        private_key=rsa.generate_private_key(public_exponent=65537, key_size=2048),
        algorithm="RS256",
        kid="key-1",
    )
    svc = _svc(signer)
    pid = uuid4()
    token = await svc.issue_token(principal_id=pid)

    doc = await jwks_document(signer)
    assert len(doc["keys"]) == 1
    assert doc["keys"][0]["kid"] == "key-1"
    assert doc["keys"][0]["use"] == "sig"

    public_key = PyJWK(doc["keys"][0]).key
    claims = jwt.decode(token, key=public_key, algorithms=["RS256"], issuer="it", audience="api")
    assert claims["sub"] == str(pid)


async def test_asymmetric_rejects_token_from_a_different_key() -> None:
    signer_a = LocalAsymmetricSigner(
        private_key=rsa.generate_private_key(public_exponent=65537, key_size=2048),
        algorithm="RS256",
    )
    signer_b = LocalAsymmetricSigner(
        private_key=rsa.generate_private_key(public_exponent=65537, key_size=2048),
        algorithm="RS256",
    )
    token = await _svc(signer_a).issue_token(principal_id=uuid4())

    with pytest.raises(CoreException) as excinfo:
        await _svc(signer_b).verify_token(token)

    assert excinfo.value.code == "invalid_access_token"


async def test_jwks_document_excludes_symmetric_signers() -> None:
    hs = Hs256Signer(secret=secrets.token_bytes(32))
    rs = LocalAsymmetricSigner(
        private_key=rsa.generate_private_key(public_exponent=65537, key_size=2048),
        algorithm="RS256",
    )

    doc = await jwks_document(hs, rs)

    # Only the asymmetric signer contributes — the HMAC secret must stay private.
    assert len(doc["keys"]) == 1
