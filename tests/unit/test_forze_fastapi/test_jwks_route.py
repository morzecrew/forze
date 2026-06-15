"""Tests for the JWKS route."""

from __future__ import annotations

import secrets

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jwt")

from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze_fastapi.routes import attach_jwks_route
from forze_identity.authn.services import (
    Hs256Signer,
    LocalAsymmetricSigner,
    jwks_document,
)

# ----------------------- #


def _client(*signers) -> TestClient:  # type: ignore[no-untyped-def]
    router = APIRouter()
    attach_jwks_route(router, lambda: jwks_document(*signers))
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def test_jwks_route_publishes_asymmetric_keys_only() -> None:
    rs = LocalAsymmetricSigner(
        private_key=rsa.generate_private_key(public_exponent=65537, key_size=2048),
        algorithm="RS256",
        kid="key-1",
    )
    hs = Hs256Signer(secret=secrets.token_bytes(32))

    resp = _client(rs, hs).get("/.well-known/jwks.json")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body["keys"]) == 1  # the HMAC secret is never published
    assert body["keys"][0]["kid"] == "key-1"
    assert body["keys"][0]["alg"] == "RS256"
    assert "max-age" in resp.headers["cache-control"]


def test_jwks_route_empty_for_symmetric_only() -> None:
    resp = _client(Hs256Signer(secret=secrets.token_bytes(32))).get(
        "/.well-known/jwks.json"
    )

    assert resp.status_code == 200
    assert resp.json() == {"keys": []}
