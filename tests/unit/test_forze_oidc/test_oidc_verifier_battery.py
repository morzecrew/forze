"""RFC 0009 §11 verifier battery, shared across the JWKS, introspection and whoami verifiers.

Two families, each run against every verifier its cases apply to:

* **Claim-level** cases (JWKS + introspection): the same claims, carried by a signed JWT or
  returned by the introspection endpoint. A Kratos session has no ``nbf``, no audience and a
  configured issuer, so whoami takes only the expiry and subject cases, in the remote family.
* **Remote** cases (introspection + whoami): the verdict comes from an endpoint, so it can
  fail, be revoked and be cached. JWKS verification is local; its fetch failing closed is
  ``test_jwks_partition_fail_closed.py``.

Alg confusion applies to the JWKS verifier alone: the other two verify no signature.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import httpx
import pytest

pytest.importorskip("jwt")

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    TokenVerifierPort,
    VerifiedAssertion,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze_identity.oidc import (
    BasicClientAuth,
    IntrospectionConfig,
    IntrospectionTokenVerifier,
    OidcTokenVerifier,
    StaticKeyProvider,
    WhoamiConfig,
    WhoamiSessionVerifier,
)
from forze_identity.oidc._cache import TokenCache

pytestmark = pytest.mark.unit

ISSUER = "https://idp.example"
AUDIENCE = "orders-api"

Verify = Callable[[dict[str, Any]], Awaitable[VerifiedAssertion]]

# ----------------------- #


def _json_response(status: int, body: object) -> httpx.Response:
    # json.dumps writes NaN / Infinity, as a hostile endpoint can; httpx's json= refuses them.
    return httpx.Response(
        status,
        content=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )


def _introspection_config(**overrides: Any) -> IntrospectionConfig:
    kwargs: dict[str, Any] = {
        "endpoint": f"{ISSUER}/introspect",
        "issuer": ISSUER,
        "audience": AUDIENCE,
        "client_auth": BasicClientAuth(client_id="rs", client_secret="rs-pass"),
    }
    kwargs.update(overrides)
    return IntrospectionConfig(**kwargs)


# ....................... #
# Claim level: JWKS + introspection


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
            lambda request: _json_response(200, {"active": True, **claims}),
        )
        verifier = IntrospectionTokenVerifier(
            config=_introspection_config(cache_ttl=timedelta(0)),
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


@pytest.mark.parametrize(
    ("claim", "value"),
    [
        pytest.param("exp", float("nan"), id="exp-nan"),
        pytest.param("exp", float("inf"), id="exp-infinity"),
        pytest.param("exp", float("-inf"), id="exp-minus-infinity"),
        pytest.param("exp", 1e20, id="exp-out-of-range"),
        pytest.param("exp", 10**400, id="exp-integer-beyond-float"),
        pytest.param("iat", float("nan"), id="iat-nan"),
        pytest.param("iat", -1e20, id="iat-out-of-range"),
        pytest.param("nbf", float("nan"), id="nbf-nan"),
        pytest.param("nbf", float("inf"), id="nbf-infinity"),
    ],
)
async def test_non_finite_or_unrepresentable_time_claim_is_authentication(
    verify: Verify,
    claim: str,
    value: float,
) -> None:
    """``NaN`` compares false both ways, so a bound check alone would admit it."""

    with pytest.raises(CoreException) as caught:
        await verify(_claims(**{claim: value}))

    assert caught.value.kind is ExceptionKind.AUTHENTICATION
    assert caught.value.code == "invalid_oidc_token"


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


# ....................... #
# Remote: introspection + whoami


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class Remote:
    """A remote verifier over a mock endpoint whose reply a test changes."""

    def __init__(self, kind: str, *, ttl: timedelta = timedelta(seconds=30)) -> None:
        self.kind = kind
        self.clock = Clock()
        self.calls = 0
        self.error: httpx.HTTPError | None = None
        self.status = 200
        self.body: object = None
        cache = TokenCache(ttl=ttl, max_entries=16, clock=self.clock)
        transport = httpx.MockTransport(self._handle)
        self.verifier: TokenVerifierPort

        if kind == "introspection":
            self.verifier = IntrospectionTokenVerifier(
                config=_introspection_config(leeway=timedelta(0)),
                transport=transport,
                cache=cache,
            )

        else:
            self.verifier = WhoamiSessionVerifier(
                config=WhoamiConfig(
                    endpoint=f"{ISSUER}/sessions/whoami",
                    issuer=ISSUER,
                    credential_style="x_session_token",
                ),
                transport=transport,
                cache=cache,
            )

        self.accept()

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1

        if self.error is not None:
            raise self.error

        return _json_response(self.status, self.body)

    def accept(self, *, expires_in: float = 300) -> None:
        expires = utcnow() + timedelta(seconds=expires_in)
        self.status = 200

        if self.kind == "introspection":
            self.body = {
                "active": True,
                "iss": ISSUER,
                "sub": "user-1",
                "aud": AUDIENCE,
                "exp": int(expires.timestamp()),
            }

        else:
            self.body = {
                "active": True,
                "expires_at": expires.isoformat(),
                "identity": {"id": "user-1", "state": "active"},
            }

    def revoke(self) -> None:
        if self.kind == "introspection":
            self.status, self.body = 200, {"active": False}

        else:
            self.status, self.body = 401, {"error": {"id": "session_inactive"}}

    async def verify(self, token: str = "credential-1") -> VerifiedAssertion:
        return await self.verifier.verify_token(AccessTokenCredentials(token=token))


@pytest.fixture(params=["introspection", "whoami"])
def remote(request: pytest.FixtureRequest) -> Remote:
    return Remote(request.param)


async def test_remote_active_verdict_is_accepted(remote: Remote) -> None:
    assertion = await remote.verify()

    assert assertion.issuer == ISSUER
    assert assertion.subject == "user-1"
    assert assertion.expires_at is not None


async def test_remote_revoked_is_authentication(remote: Remote) -> None:
    remote.revoke()

    with pytest.raises(CoreException) as caught:
        await remote.verify()

    assert caught.value.kind is ExceptionKind.AUTHENTICATION


async def test_remote_expired_is_authentication(remote: Remote) -> None:
    remote.accept(expires_in=-120)

    with pytest.raises(CoreException) as caught:
        await remote.verify()

    assert caught.value.kind is ExceptionKind.AUTHENTICATION


@pytest.mark.parametrize("status", [500, 502, 503, 504])
async def test_remote_5xx_fails_closed(remote: Remote, status: int) -> None:
    remote.status = status

    with pytest.raises(CoreException) as caught:
        await remote.verify()

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE


@pytest.mark.parametrize(
    "error",
    [httpx.ConnectError("refused"), httpx.ReadTimeout("slow")],
    ids=["connect-error", "timeout"],
)
async def test_remote_unreachable_fails_closed(remote: Remote, error: httpx.HTTPError) -> None:
    remote.error = error

    with pytest.raises(CoreException) as caught:
        await remote.verify()

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE


async def test_remote_revoked_dies_at_ttl_never_later(remote: Remote) -> None:
    await remote.verify()
    remote.revoke()

    remote.clock.now += 29.9
    assert (await remote.verify()).subject == "user-1"
    assert remote.calls == 1

    remote.clock.now += 0.1
    with pytest.raises(CoreException):
        await remote.verify()

    assert remote.calls == 2


async def test_remote_cache_is_clamped_by_expiry(remote: Remote) -> None:
    remote.accept(expires_in=5)
    await remote.verify()

    remote.clock.now += 2
    await remote.verify()
    assert remote.calls == 1

    remote.clock.now += 4  # past the credential's expiry, well inside the 30 s TTL
    remote.revoke()
    with pytest.raises(CoreException):
        await remote.verify()

    assert remote.calls == 2


async def test_remote_failures_are_not_cached(remote: Remote) -> None:
    remote.status = 503

    for _ in range(2):
        with pytest.raises(CoreException):
            await remote.verify()

    assert remote.calls == 2


async def test_remote_cache_keys_are_digests_never_credentials(remote: Remote) -> None:
    token = secrets.token_hex(16)

    await remote.verify(token)

    cache: TokenCache = remote.verifier.cache  # type: ignore[attr-defined]
    keys = list(cache._entries)  # pyright: ignore[reportPrivateUsage]
    assert keys == [hashlib.sha256(token.encode()).digest()]
    assert token.encode() not in b"".join(keys)
    assert token not in repr(remote.verifier)
