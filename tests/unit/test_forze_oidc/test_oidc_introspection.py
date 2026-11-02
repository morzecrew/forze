"""RFC 7662 introspection verifier: request shape, fail-closed verdicts, bounded digest cache."""

from __future__ import annotations

import base64
import secrets
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import parse_qs

import httpx
import pytest

pytest.importorskip("jwt")

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from pydantic import SecretStr

from forze.application.contracts.authn import AccessTokenCredentials, AuthnSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze_identity.oidc import (
    FORZE_CLAIMS,
    BasicClientAuth,
    ClaimsPath,
    ConfigurableIntrospectionVerifier,
    IntrospectionConfig,
    IntrospectionTokenVerifier,
    OidcClaimMapper,
    PrivateKeyJwtAuth,
)
from forze_identity.oidc._cache import TokenCache

pytestmark = pytest.mark.unit

ISSUER = "https://idp.example"
ENDPOINT = f"{ISSUER}/oauth/introspect"
AUDIENCE = "orders-api"

# ----------------------- #


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class Idp:
    """A mock introspection endpoint whose verdict a test can flip."""

    def __init__(self, body: Any = None, *, status: int = 200) -> None:
        self.body = body
        self.status = status
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        body = self.body if self.body is not None else _active()
        return httpx.Response(self.status, json=body)

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)


def _active(**overrides: Any) -> dict[str, Any]:
    now = int(utcnow().timestamp())
    body: dict[str, Any] = {
        "active": True,
        "iss": ISSUER,
        "sub": "user-1",
        "aud": AUDIENCE,
        "exp": now + 3600,
        "client_id": "app",
        "scope": "openid",
    }
    body.update(overrides)
    return {k: v for k, v in body.items() if v is not None}


def _config(**overrides: Any) -> IntrospectionConfig:
    kwargs: dict[str, Any] = {
        "endpoint": ENDPOINT,
        "issuer": ISSUER,
        "audience": AUDIENCE,
        "client_auth": BasicClientAuth(client_id="rs", client_secret=SecretStr("rs-pass")),
    }
    kwargs.update(overrides)
    return IntrospectionConfig(**kwargs)


def _verifier(
    idp: Idp,
    *,
    clock: Clock | None = None,
    ttl: timedelta = timedelta(seconds=30),
    max_entries: int = 1024,
    **config: Any,
) -> IntrospectionTokenVerifier:
    return IntrospectionTokenVerifier(
        config=_config(**config),
        transport=idp.transport,
        cache=TokenCache(ttl=ttl, max_entries=max_entries, clock=clock or Clock()),
    )


def _creds(token: str = "opaque-token") -> AccessTokenCredentials:
    return AccessTokenCredentials(token=token)


# ----------------------- #


async def test_posts_token_with_basic_client_auth() -> None:
    idp = Idp()

    assertion = await _verifier(idp).verify_token(_creds("tok-123"))

    (request,) = idp.requests
    assert request.method == "POST"
    assert str(request.url) == ENDPOINT
    assert parse_qs(request.content.decode()) == {"token": ["tok-123"]}
    assert request.headers["authorization"] == "Basic " + base64.b64encode(b"rs:rs-pass").decode()
    assert assertion.subject == "user-1"
    assert assertion.issuer == ISSUER
    assert assertion.claims["client_id"] == "app"


def test_basic_auth_form_encodes_credentials() -> None:
    """RFC 6749 §2.3.1: id and secret are form-urlencoded before the Basic encoding."""

    _, headers = BasicClientAuth(client_id="a:b", client_secret="p@ss word").request_parts()

    decoded = base64.b64decode(headers["Authorization"].removeprefix("Basic ")).decode()
    assert decoded == "a%3Ab:p%40ss+word"


async def test_private_key_jwt_client_assertion() -> None:
    key = ec.generate_private_key(ec.SECP256R1())
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    idp = Idp()
    auth = PrivateKeyJwtAuth(
        client_id="rs",
        private_key=SecretStr(pem),
        audience=ISSUER,
        key_id="kid-1",
        algorithm="ES256",
    )

    await _verifier(idp, client_auth=auth).verify_token(_creds())

    form = parse_qs(idp.requests[0].content.decode())
    assert "authorization" not in idp.requests[0].headers
    assert form["client_assertion_type"] == [
        "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
    ]
    assertion = form["client_assertion"][0]
    assert jwt.get_unverified_header(assertion)["kid"] == "kid-1"
    claims = jwt.decode(assertion, key.public_key(), algorithms=["ES256"], audience=ISSUER)
    assert claims["iss"] == claims["sub"] == "rs"
    assert claims["exp"] - claims["iat"] == 60

    # A fresh jti per assertion: an issuer that stores seen ones refuses a repeat.
    second = jwt.decode(auth.assertion(), key.public_key(), algorithms=["ES256"], audience=ISSUER)
    assert claims["jti"] and second["jti"] and claims["jti"] != second["jti"]


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        pytest.param({"algorithm": "HS256"}, "algorithm", id="hmac-algorithm"),
        pytest.param({"private_key": "not a pem"}, "does not sign", id="unloadable-key"),
        pytest.param({"audience": " "}, "audience", id="blank-audience"),
    ],
)
def test_private_key_jwt_refused_at_construction(kwargs: dict[str, Any], match: str) -> None:
    base: dict[str, Any] = {"client_id": "rs", "private_key": "x", "audience": ISSUER}

    with pytest.raises(CoreException, match=match) as caught:
        PrivateKeyJwtAuth(**{**base, **kwargs})

    assert caught.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({"active": False}, id="inactive"),
        pytest.param({}, id="no-active-field"),
        pytest.param(_active(active="true"), id="active-is-string"),
        pytest.param(_active(active=1), id="active-is-int"),
        pytest.param(_active(error="invalid_token"), id="2xx-with-error"),
        pytest.param(_active(sub=""), id="empty-subject"),
        pytest.param(_active(sub=7), id="subject-not-string"),
        pytest.param(_active(iss="https://evil.example"), id="foreign-issuer"),
        pytest.param(_active(exp="soon"), id="exp-not-number"),
        pytest.param(_active(exp=True), id="exp-bool"),
        pytest.param(_active(nbf="later"), id="nbf-not-number"),
        pytest.param(_active(iat="then"), id="iat-not-number"),
        pytest.param(_active(aud=[AUDIENCE, 7]), id="aud-list-with-non-string"),
        pytest.param(_active(aud=[AUDIENCE, None]), id="aud-list-with-null"),
    ],
)
async def test_inactive_or_unacceptable_verdict_is_one_authentication_shape(body: Any) -> None:
    with pytest.raises(CoreException) as caught:
        await _verifier(Idp(body)).verify_token(_creds())

    assert caught.value.kind is ExceptionKind.AUTHENTICATION
    assert caught.value.code == "invalid_oidc_token"


async def test_missing_iss_takes_the_configured_issuer() -> None:
    assertion = await _verifier(Idp(_active(iss=None))).verify_token(_creds())

    assert assertion.issuer == ISSUER


@pytest.mark.parametrize("status", [400, 401, 403, 404, 500, 502, 503])
async def test_non_2xx_fails_closed_as_infrastructure(status: int) -> None:
    with pytest.raises(CoreException) as caught:
        await _verifier(Idp(_active(), status=status)).verify_token(_creds())

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE
    assert caught.value.code == "oidc_introspection_unavailable"


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("slow"),
        httpx.ConnectTimeout("slow"),
    ],
)
async def test_transport_failure_and_timeout_fail_closed(error: httpx.HTTPError) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error

    verifier = IntrospectionTokenVerifier(
        config=_config(),
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(CoreException) as caught:
        await verifier.verify_token(_creds())

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE


@pytest.mark.parametrize(
    "content",
    [b"<html>oops</html>", b"[1, 2]", b'"active"', b""],
)
async def test_malformed_body_fails_closed(content: bytes) -> None:
    verifier = IntrospectionTokenVerifier(
        config=_config(),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=content)),
    )

    with pytest.raises(CoreException) as caught:
        await verifier.verify_token(_creds())

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE
    assert caught.value.code == "oidc_introspection_invalid_response"


async def test_no_audience_accepted_only_when_explicitly_not_required() -> None:
    with pytest.raises(CoreException, match="audience"):
        _config(audience=None)

    verifier = _verifier(Idp(_active(aud=None)), audience=None, require_audience=False)

    assertion = await verifier.verify_token(_creds())

    assert assertion.audience is None


async def test_claim_mapper_extracts_roles_from_the_response() -> None:
    body = _active(realm_access={"roles": ["billing-admin"]}, email="a@example.com")
    mapper = OidcClaimMapper(roles=ClaimsPath("realm_access.roles"), email_claim="email")

    assertion = await _verifier(Idp(body), claim_mapper=mapper).verify_token(_creds())

    assert assertion.claims[FORZE_CLAIMS] == {"email": "a@example.com", "roles": ["billing-admin"]}


# ....................... #
# Revocation at the TTL, the expiry clamp and digest-only keys are in the shared battery.


async def test_negative_verdicts_are_not_cached() -> None:
    idp = Idp({"active": False})
    verifier = _verifier(idp)

    for _ in range(2):
        with pytest.raises(CoreException):
            await verifier.verify_token(_creds())

    assert len(idp.requests) == 2


async def test_zero_ttl_disables_the_cache() -> None:
    idp = Idp()
    verifier = _verifier(idp, ttl=timedelta(0))

    await verifier.verify_token(_creds())
    await verifier.verify_token(_creds())

    assert len(idp.requests) == 2


async def test_cache_is_bounded_least_recently_used_first() -> None:
    idp = Idp()
    verifier = _verifier(idp, max_entries=2)

    for token in ("t1", "t2", "t1", "t3"):  # t1 refreshed, so t2 is evicted
        await verifier.verify_token(_creds(token))

    assert len(idp.requests) == 3

    await verifier.verify_token(_creds("t1"))
    assert len(idp.requests) == 3

    await verifier.verify_token(_creds("t2"))
    assert len(idp.requests) == 4


@pytest.mark.parametrize("ttl", [timedelta(seconds=-1), timedelta(minutes=5, seconds=1)])
def test_cache_ttl_outside_bounds_refused(ttl: timedelta) -> None:
    with pytest.raises(CoreException, match="TTL"):
        IntrospectionTokenVerifier(config=_config(cache_ttl=ttl))


def test_cache_max_entries_must_be_positive() -> None:
    with pytest.raises(CoreException, match="max entries"):
        IntrospectionTokenVerifier(config=_config(cache_max_entries=0))


# ....................... #


async def test_secrets_and_tokens_stay_out_of_errors_and_reprs() -> None:
    token = secrets.token_hex(16)
    password = secrets.token_hex(16)
    auth = BasicClientAuth(client_id="rs", client_secret=password)

    assert password not in repr(auth)
    assert password not in repr(_config(client_auth=auth))

    for idp in (Idp({"active": False}), Idp(_active(), status=401)):
        verifier = _verifier(idp, client_auth=auth)

        with pytest.raises(CoreException) as caught:
            await verifier.verify_token(_creds(token))

        rendered = f"{caught.value!s} {caught.value.details!r}"
        assert token not in rendered
        assert password not in rendered


async def test_missing_claim_the_mapper_requires_is_authentication() -> None:
    verifier = _verifier(Idp(), claim_mapper=OidcClaimMapper(subject_claim="uid"))

    with pytest.raises(CoreException) as caught:
        await verifier.verify_token(_creds())

    assert caught.value.kind is ExceptionKind.AUTHENTICATION
    assert caught.value.code == "invalid_oidc_token"


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        pytest.param({"audience": []}, "audience", id="empty-audience-list"),
        pytest.param({"audience": ("api", "")}, "audience", id="blank-audience-member"),
        pytest.param({"audience": 42}, "audience", id="non-sequence-audience"),
        pytest.param(
            {"claim_mapper": OidcClaimMapper(issuer_claim="issuer")}, "iss", id="custom-iss"
        ),
        *(
            pytest.param({"timeout": t}, "timeout", id=f"timeout-{t!r}")
            for t in (float("nan"), float("inf"), 0, -1.0, True, "10", 10**400)
        ),
    ],
)
def test_configuration_refused(kwargs: dict[str, Any], match: str) -> None:
    with pytest.raises(CoreException, match=match) as caught:
        _config(**kwargs)

    assert caught.value.kind is ExceptionKind.CONFIGURATION


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://idp.example/introspect",
        "idp.example/introspect",
        "https://",
        "https://idp.example:port/introspect",
        "https://idp.example:99999/introspect",
    ],
)
def test_insecure_endpoint_refused(endpoint: str) -> None:
    with pytest.raises(CoreException) as caught:
        _config(endpoint=endpoint)

    assert caught.value.kind is ExceptionKind.CONFIGURATION


def test_factory_builds_the_verifier_once() -> None:
    factory = ConfigurableIntrospectionVerifier(config=_config())
    spec = AuthnSpec(name="api", enabled_methods=frozenset({"token"}))

    first = factory(MagicMock(), spec)

    assert isinstance(first, IntrospectionTokenVerifier)
    assert factory(MagicMock(), spec) is first
