"""Tests for shared OIDC bootstrap deps and preset wiring."""

from __future__ import annotations

import secrets
from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("jwt")

import jwt

from forze.application.contracts.authn import AccessTokenCredentials, AuthnDepKey
from forze.base.primitives import utcnow
from forze_identity.authn import DeterministicUuidResolver
from forze_identity.builtin.idp._oidc import oidc_bootstrap_identity_deps
from forze_identity.builtin.idp.google import (
    GOOGLE_OIDC_ISSUER,
    GoogleOidcConfig,
    google_identity_deps,
)
from forze_identity.oidc import OidcTokenVerifier, StaticKeyProvider

pytestmark = pytest.mark.unit


def _hs256_id_token(
    secret: bytes,
    *,
    issuer: str,
    subject: str,
    audience: str,
) -> str:
    now = utcnow()
    payload = {
        "iss": issuer,
        "sub": subject,
        "aud": audience,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def test_google_identity_deps_registers_bootstrap_route() -> None:
    config = GoogleOidcConfig(client_id="google-client")
    deps = google_identity_deps(config, authn_route="bootstrap")

    assert deps.exists(AuthnDepKey, route="bootstrap")


def test_oidc_bootstrap_identity_deps_custom_route() -> None:
    config = GoogleOidcConfig(client_id="app")
    deps = oidc_bootstrap_identity_deps(
        authn_route="login",
        token_verifier=__import__(
            "forze_identity.builtin.idp.google.deps",
            fromlist=["ConfigurableGoogleOidcVerifier"],
        ).ConfigurableGoogleOidcVerifier(config=config),
    )

    assert deps.exists(AuthnDepKey, route="login")


@pytest.mark.asyncio
async def test_google_preset_issuer_audience_with_static_keys() -> None:
    secret = secrets.token_bytes(32)
    client_id = "google-client-123"
    config = GoogleOidcConfig(client_id=client_id)
    preset = config.to_preset()

    assert preset.issuer == GOOGLE_OIDC_ISSUER
    assert preset.audience == client_id

    verifier = OidcTokenVerifier(
        key_provider=StaticKeyProvider(key=secret),
        algorithms=("HS256",),
        issuer=preset.issuer,
        audience=preset.audience,
        enforce_issuer_and_audience=True,
    )

    sub = str(uuid4())
    token = _hs256_id_token(
        secret,
        issuer=preset.issuer,
        subject=sub,
        audience=client_id,
    )
    assertion = await verifier.verify_token(AccessTokenCredentials(token=token))
    identity = await DeterministicUuidResolver().resolve(assertion)

    assert assertion.subject == sub
    assert identity.principal_id is not None
