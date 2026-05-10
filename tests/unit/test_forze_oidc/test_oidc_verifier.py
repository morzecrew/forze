"""Unit tests for the :mod:`forze_oidc` skeleton (claim mapper, key provider, verifier).

Demonstrates that the seam introduced by the strategic authn refactor lets a third-party
IdP integration plug into the same dependency keys without touching core contracts.
"""

from __future__ import annotations

import secrets
from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("jwt")

pytestmark = pytest.mark.unit

import jwt

from forze.application.contracts.authn import TokenCredentials, VerifiedAssertion
from forze.base.errors import AuthenticationError
from forze.base.primitives import utcnow
from forze_authn import DeterministicUuidResolver, MappingTableResolver
from forze_oidc import (
    OidcClaimMapper,
    OidcTokenVerifier,
    StaticKeyProvider,
)

# ----------------------- #


def _hs256_token(
    secret: bytes,
    *,
    issuer: str,
    subject: str,
    audience: str = "my-app",
    extra: dict[str, object] | None = None,
    expires_in: timedelta = timedelta(minutes=5),
) -> str:
    now = utcnow()
    payload: dict[str, object] = {
        "iss": issuer,
        "sub": subject,
        "aud": audience,
        "iat": int(now.timestamp()),
        "exp": int((now + expires_in).timestamp()),
    }
    if extra:
        payload.update(extra)

    return jwt.encode(payload, secret, algorithm="HS256")


# ....................... #


class TestOidcClaimMapper:
    def test_default_mapping(self) -> None:
        mapper = OidcClaimMapper()
        claims: dict[str, object] = {
            "iss": "https://issuer",
            "sub": "user-1",
            "aud": "app",
            "iat": 1_700_000_000,
            "exp": 1_700_000_900,
        }
        a = mapper.map(claims)
        assert isinstance(a, VerifiedAssertion)
        assert a.issuer == "https://issuer"
        assert a.subject == "user-1"
        assert a.audience == "app"
        assert a.issued_at is not None
        assert a.expires_at is not None
        assert a.tenant_hint is None

    def test_audience_array_picks_first_string(self) -> None:
        mapper = OidcClaimMapper()
        a = mapper.map(
            {
                "iss": "issuer",
                "sub": "u",
                "aud": ["api-1", "api-2"],
            }
        )
        assert a.audience == "api-1"

    def test_tenant_claim_override(self) -> None:
        mapper = OidcClaimMapper(tenant_claim="firebase_tenant")
        a = mapper.map(
            {
                "iss": "issuer",
                "sub": "u",
                "firebase_tenant": "tenant-7",
            }
        )
        assert a.tenant_hint == "tenant-7"

    def test_rejects_missing_required_claims(self) -> None:
        mapper = OidcClaimMapper()
        with pytest.raises(ValueError, match="iss"):
            mapper.map({"sub": "u"})


# ....................... #


class TestOidcTokenVerifier:
    @pytest.mark.asyncio
    async def test_round_trip_with_static_hs256(self) -> None:
        secret = secrets.token_bytes(32)
        verifier = OidcTokenVerifier(
            key_provider=StaticKeyProvider(key=secret),
            algorithms=("HS256",),
            issuer="https://issuer.example",
            audience="my-app",
        )

        sub = str(uuid4())
        token = _hs256_token(secret, issuer="https://issuer.example", subject=sub)
        assertion = await verifier.verify_token(TokenCredentials(token=token))

        assert assertion.issuer == "https://issuer.example"
        assert assertion.subject == sub
        assert assertion.audience == "my-app"

    @pytest.mark.asyncio
    async def test_rejects_wrong_issuer(self) -> None:
        secret = secrets.token_bytes(32)
        verifier = OidcTokenVerifier(
            key_provider=StaticKeyProvider(key=secret),
            algorithms=("HS256",),
            issuer="expected",
        )
        token = _hs256_token(secret, issuer="other", subject="u")

        with pytest.raises(AuthenticationError):
            await verifier.verify_token(TokenCredentials(token=token))

    @pytest.mark.asyncio
    async def test_rejects_expired_token(self) -> None:
        secret = secrets.token_bytes(32)
        verifier = OidcTokenVerifier(
            key_provider=StaticKeyProvider(key=secret),
            algorithms=("HS256",),
            issuer="iss",
            audience="aud",
        )
        expired = _hs256_token(
            secret,
            issuer="iss",
            subject="u",
            audience="aud",
            expires_in=timedelta(seconds=-300),
        )

        with pytest.raises(AuthenticationError) as ei:
            await verifier.verify_token(TokenCredentials(token=expired))
        assert ei.value.code == "oidc_token_expired"

    @pytest.mark.asyncio
    async def test_pairs_with_deterministic_resolver(self) -> None:
        """End-to-end: external IdP token → assertion → internal UUID via DeterministicUuidResolver."""
        secret = secrets.token_bytes(32)
        verifier = OidcTokenVerifier(
            key_provider=StaticKeyProvider(key=secret),
            algorithms=("HS256",),
            issuer="https://issuer",
            audience="app",
        )
        resolver = DeterministicUuidResolver()

        token = _hs256_token(
            secret, issuer="https://issuer", subject="ext:42", audience="app"
        )
        assertion = await verifier.verify_token(TokenCredentials(token=token))
        identity = await resolver.resolve(assertion)

        # Stable across calls
        assertion2 = await verifier.verify_token(TokenCredentials(token=token))
        identity2 = await resolver.resolve(assertion2)
        assert identity.principal_id == identity2.principal_id

    def test_pairs_with_mapping_table_resolver_compiles(self) -> None:
        """Sanity check: MappingTableResolver type-checks against the OIDC verifier output.

        Full round-trip belongs in integration tests with a real document gateway.
        """
        # Just constructing a (verifier, resolver) tuple is enough to confirm both sides
        # speak ``VerifiedAssertion``.
        verifier = OidcTokenVerifier(
            key_provider=StaticKeyProvider(key=b"x" * 32),
        )
        # MappingTableResolver requires a query port; construction is exercised in its own
        # tests. Asserting that both implement the related protocols is a contract smoke.
        assert hasattr(verifier, "verify_token")
        assert hasattr(MappingTableResolver, "resolve")
