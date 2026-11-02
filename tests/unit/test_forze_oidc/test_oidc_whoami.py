"""Kratos whoami session verifier: credential forwarding, fail-closed verdicts, bounded cache."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

pytest.importorskip("jwt")

from forze.application.contracts.authn import AccessTokenCredentials, AuthnSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow
from forze_identity.oidc import (
    FORZE_CLAIMS,
    ClaimsPath,
    ConfigurableWhoamiVerifier,
    OidcClaimMapper,
    WhoamiConfig,
    WhoamiSessionVerifier,
)
from forze_identity.oidc._cache import TokenCache

pytestmark = pytest.mark.unit

ISSUER = "https://kratos.example"
ENDPOINT = f"{ISSUER}/sessions/whoami"
IDENTITY_ID = "9f425a8d-7efc-4768-8f23-7647a74fdf13"

# ----------------------- #


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class Kratos:
    def __init__(self, body: Any = None, *, status: int = 200) -> None:
        self.body = body
        self.status = status
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.status, json=self.body if self.body is not None else _session())

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)


def _iso(delta: timedelta) -> str:
    return (utcnow() + delta).isoformat().replace("+00:00", "Z")


def _session(**overrides: Any) -> dict[str, Any]:
    session: dict[str, Any] = {
        "id": "sess-1",
        "active": True,
        "expires_at": _iso(timedelta(hours=1)),
        "authenticated_at": _iso(timedelta(minutes=-5)),
        "issued_at": _iso(timedelta(minutes=-5)),
        "identity": {
            "id": IDENTITY_ID,
            "state": "active",
            "schema_id": "default",
            "traits": {"email": "ann@example.com", "name": "Ann"},
            "metadata_public": {"roles": ["support"]},
        },
    }
    session.update(overrides)
    return session


def _verifier(
    kratos: Kratos,
    *,
    clock: Clock | None = None,
    ttl: timedelta = timedelta(seconds=30),
    **config: Any,
) -> WhoamiSessionVerifier:
    kwargs: dict[str, Any] = {
        "endpoint": ENDPOINT,
        "issuer": ISSUER,
        "credential_style": "x_session_token",
    }
    kwargs.update(config)
    return WhoamiSessionVerifier(
        config=WhoamiConfig(**kwargs),
        transport=kratos.transport,
        cache=TokenCache(ttl=ttl, max_entries=16, clock=clock or Clock()),
    )


def _creds(token: str = "ory_st_abc123") -> AccessTokenCredentials:
    return AccessTokenCredentials(token=token)


# ----------------------- #


async def test_session_token_maps_identity_to_assertion() -> None:
    kratos = Kratos()
    mapper = OidcClaimMapper(email_claim="traits.email", roles=ClaimsPath("metadata.roles"))

    assertion = await _verifier(kratos, claim_mapper=mapper).verify_token(_creds())

    (request,) = kratos.requests
    assert request.method == "GET"
    assert request.headers["x-session-token"] == "ory_st_abc123"
    assert "cookie" not in request.headers
    assert assertion.issuer == ISSUER
    assert assertion.subject == IDENTITY_ID
    assert assertion.expires_at is not None and assertion.expires_at > utcnow()
    assert assertion.issued_at is not None
    assert assertion.claims["sid"] == "sess-1"
    assert assertion.claims["traits"] == {"email": "ann@example.com", "name": "Ann"}
    assert assertion.claims["metadata"] == {"roles": ["support"]}
    assert assertion.claims[FORZE_CLAIMS] == {"email": "ann@example.com", "roles": ["support"]}


async def test_cookie_style_forwards_the_named_cookie() -> None:
    kratos = Kratos()
    verifier = _verifier(kratos, credential_style="cookie", cookie_name="my_session")

    await verifier.verify_token(_creds("MTY5NjA4-cookie_value=="))

    assert kratos.requests[0].headers["cookie"] == "my_session=MTY5NjA4-cookie_value=="
    assert "x-session-token" not in kratos.requests[0].headers


async def test_traits_left_out_when_disabled() -> None:
    assertion = await _verifier(Kratos(), traits_to_claims=False).verify_token(_creds())

    assert "traits" not in assertion.claims
    assert "metadata" not in assertion.claims


@pytest.mark.parametrize("status", [401, 403])
async def test_no_or_insufficient_session_is_authentication(status: int) -> None:
    with pytest.raises(CoreException) as caught:
        await _verifier(Kratos({"error": {"id": "session_inactive"}}, status=status)).verify_token(
            _creds()
        )

    assert caught.value.kind is ExceptionKind.AUTHENTICATION
    assert caught.value.code == "invalid_session_token"


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(_session(active=False), id="inactive-session"),
        pytest.param({k: v for k, v in _session().items() if k != "active"}, id="no-active"),
        pytest.param(_session(identity=None), id="no-identity"),
        pytest.param(_session(identity={"id": ""}), id="empty-identity-id"),
        pytest.param(_session(identity={"id": 7}), id="identity-id-not-string"),
        pytest.param(_session(identity={"id": IDENTITY_ID, "state": "inactive"}), id="inactive-id"),
        pytest.param(_session(identity={"id": IDENTITY_ID}), id="identity-without-state"),
        pytest.param(_session(identity={"id": IDENTITY_ID, "state": None}), id="null-state"),
        pytest.param(_session(expires_at=_iso(timedelta(seconds=-1))), id="expired"),
    ],
)
async def test_unacceptable_session_is_authentication(body: dict[str, Any]) -> None:
    with pytest.raises(CoreException) as caught:
        await _verifier(Kratos(body)).verify_token(_creds())

    assert caught.value.kind is ExceptionKind.AUTHENTICATION


@pytest.mark.parametrize(
    "token",
    ["", "a b", "a;other=1", "a\r\nX-Injected: 1", 'a"b', "a,b", "a\\b", "café"],
)
async def test_unsafe_credential_rejected_before_any_request(token: str) -> None:
    kratos = Kratos()

    with pytest.raises(CoreException) as caught:
        await _verifier(kratos, credential_style="cookie").verify_token(_creds(token))

    assert caught.value.kind is ExceptionKind.AUTHENTICATION
    assert kratos.requests == []


@pytest.mark.parametrize("status", [404, 500, 502, 503])
async def test_other_status_fails_closed_as_infrastructure(status: int) -> None:
    with pytest.raises(CoreException) as caught:
        await _verifier(Kratos(_session(), status=status)).verify_token(_creds())

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE
    assert caught.value.code == "session_whoami_unavailable"


async def test_timeout_fails_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    verifier = WhoamiSessionVerifier(
        config=WhoamiConfig(endpoint=ENDPOINT, issuer=ISSUER, credential_style="cookie"),
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(CoreException) as caught:
        await verifier.verify_token(_creds())

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(["x"], id="json-array"),
        pytest.param(_session(expires_at="tomorrow"), id="bad-expiry"),
        pytest.param(_session(expires_at="2030-01-01T00:00:00"), id="naive-expiry"),
        pytest.param(_session(expires_at=1_900_000_000), id="numeric-expiry"),
    ],
)
async def test_malformed_body_fails_closed(body: Any) -> None:
    with pytest.raises(CoreException) as caught:
        await _verifier(Kratos(body)).verify_token(_creds())

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE
    assert caught.value.code == "session_whoami_invalid_response"


# Revocation at the TTL, the expiry clamp and digest-only keys are in the shared battery.


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        pytest.param({"endpoint": "http://kratos.example/sessions/whoami"}, "https", id="http"),
        pytest.param({"credential_style": "bearer"}, "credential_style", id="unknown-style"),
        pytest.param({"cookie_name": "a=b"}, "cookie_name", id="cookie-name-with-equals"),
        pytest.param({"cookie_name": "a b"}, "cookie_name", id="cookie-name-with-space"),
        pytest.param({"issuer": " "}, "issuer", id="blank-issuer"),
        pytest.param({"cache_ttl": timedelta(hours=1)}, "TTL", id="ttl-too-long"),
    ],
)
def test_configuration_refused(kwargs: dict[str, Any], match: str) -> None:
    base: dict[str, Any] = {"endpoint": ENDPOINT, "issuer": ISSUER, "credential_style": "cookie"}

    with pytest.raises(CoreException, match=match):
        WhoamiSessionVerifier(config=WhoamiConfig(**{**base, **kwargs}))


def test_factory_builds_the_verifier_once() -> None:
    config = WhoamiConfig(endpoint=ENDPOINT, issuer=ISSUER, credential_style="cookie")
    factory = ConfigurableWhoamiVerifier(config=config)
    spec = AuthnSpec(name="web", enabled_methods=frozenset({"token"}))

    first = factory(MagicMock(), spec)

    assert isinstance(first, WhoamiSessionVerifier)
    assert factory(MagicMock(), spec) is first
