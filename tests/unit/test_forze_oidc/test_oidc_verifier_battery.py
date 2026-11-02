"""RFC 0009 §11 verifier battery, shared across the JWKS and introspection verifiers.

The same claim-level cases run against both: a JWT verified against keys, and an opaque token
whose claims come back from the introspection endpoint. Introspection-only and whoami-only
cases (``active:false``, 5xx, cache bounds) live beside their verifiers' own tests.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import httpx
import pytest

pytest.importorskip("jwt")

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from forze.application.contracts.authn import AccessTokenCredentials, VerifiedAssertion
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze_identity.oidc import (
    BasicClientAuth,
    IntrospectionConfig,
    IntrospectionTokenVerifier,
    OidcTokenVerifier,
    StaticKeyProvider,
)

pytestmark = pytest.mark.unit

ISSUER = "https://idp.example"
AUDIENCE = "orders-api"

Verify = Callable[[dict[str, Any]], Awaitable[VerifiedAssertion]]

# ----------------------- #


@pytest.fixture(scope="module")
def rsa_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _public_pem(key: rsa.RSAPrivateKey) -> bytes:
    return key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def _jwks_verify(key: rsa.RSAPrivateKey) -> Verify:
    verifier = OidcTokenVerifier(
        key_provider=StaticKeyProvider(key=_public_pem(key)),
        algorithms=("RS256",),
        issuer=ISSUER,
        audience=AUDIENCE,
    )

    async def verify(claims: dict[str, Any]) -> VerifiedAssertion:
        token = jwt.encode(claims, key, algorithm="RS256")
        return await verifier.verify_token(AccessTokenCredentials(token=token))

    return verify


def _introspection_verify() -> Verify:
    async def verify(claims: dict[str, Any]) -> VerifiedAssertion:
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, json={"active": True, **claims}),
        )
        verifier = IntrospectionTokenVerifier(
            config=IntrospectionConfig(
                endpoint=f"{ISSUER}/introspect",
                issuer=ISSUER,
                audience=AUDIENCE,
                client_auth=BasicClientAuth(client_id="rs", client_secret="rs-pass"),
                cache_ttl=timedelta(0),
            ),
            transport=transport,
        )
        return await verifier.verify_token(AccessTokenCredentials(token="opaque-token"))

    return verify


@pytest.fixture(params=["jwks", "introspection"])
def verify(request: pytest.FixtureRequest, rsa_key: rsa.RSAPrivateKey) -> Verify:
    return _jwks_verify(rsa_key) if request.param == "jwks" else _introspection_verify()


def _claims(**overrides: Any) -> dict[str, Any]:
    now = int(utcnow().timestamp())
    claims: dict[str, Any] = {
        "iss": ISSUER,
        "sub": "user-1",
        "aud": AUDIENCE,
        "iat": now,
        "exp": now + 300,
    }
    claims.update(overrides)
    return {k: v for k, v in claims.items() if v is not None}


# ----------------------- #


async def test_valid_token_is_accepted(verify: Verify) -> None:
    assertion = await verify(_claims())

    assert assertion.issuer == ISSUER
    assert assertion.subject == "user-1"
    assert assertion.audience == AUDIENCE


async def test_multi_audience_records_the_validated_one(verify: Verify) -> None:
    assertion = await verify(_claims(aud=["other-party", AUDIENCE]))

    assert assertion.audience == AUDIENCE


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"exp": -120}, id="expired"),
        pytest.param({"nbf": 120}, id="not-yet-valid"),
        pytest.param({"aud": "someone-else"}, id="wrong-audience"),
        pytest.param({"aud": ["a", "b"]}, id="wrong-audience-list"),
        pytest.param({"aud": None}, id="missing-audience"),
        pytest.param({"iss": "https://evil.example"}, id="wrong-issuer"),
        pytest.param({"sub": None}, id="missing-subject"),
    ],
)
async def test_rejected_as_authentication(verify: Verify, overrides: dict[str, Any]) -> None:
    now = int(utcnow().timestamp())
    relative = {k: now + v for k, v in overrides.items() if k in ("exp", "nbf")}

    with pytest.raises(CoreException) as caught:
        await verify(_claims(**{**overrides, **relative}))

    assert caught.value.kind is ExceptionKind.AUTHENTICATION


async def test_alg_confusion_hs256_against_rs256_allowlist(rsa_key: rsa.RSAPrivateKey) -> None:
    """An HS256 token MAC'd with the public key must not pass an RS256-only verifier."""

    public_pem = _public_pem(rsa_key)
    verifier = OidcTokenVerifier(
        key_provider=StaticKeyProvider(key=public_pem),
        algorithms=("RS256",),
        issuer=ISSUER,
        audience=AUDIENCE,
    )
    header = jwt.utils.base64url_encode(b'{"alg":"HS256","typ":"JWT"}')
    payload = jwt.utils.base64url_encode(json.dumps(_claims()).encode())
    signing_input = header + b"." + payload
    signature = jwt.utils.base64url_encode(
        hmac.new(public_pem, signing_input, hashlib.sha256).digest(),
    )
    forged = (signing_input + b"." + signature).decode()

    with pytest.raises(CoreException) as caught:
        await verifier.verify_token(AccessTokenCredentials(token=forged))

    assert caught.value.kind is ExceptionKind.AUTHENTICATION
    assert caught.value.code == "invalid_oidc_token"
