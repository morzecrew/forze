"""OIDC discovery: exact issuer match, https-only endpoints, construction-time only."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

pytest.importorskip("jwt")

from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.oidc import (
    ClaimsPath,
    ConfigurableOidcIdpVerifier,
    OidcClaimMapper,
    OidcIdpPreset,
    OidcProviderMetadata,
    fetch_oidc_configuration,
)

pytestmark = pytest.mark.unit

ISSUER = "https://idp.example/realms/acme"

# ----------------------- #


def _document(**overrides: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "issuer": ISSUER,
        "jwks_uri": f"{ISSUER}/certs",
        "introspection_endpoint": f"{ISSUER}/introspect",
        "token_endpoint": f"{ISSUER}/token",
        "userinfo_endpoint": f"{ISSUER}/userinfo",
        "id_token_signing_alg_values_supported": ["RS256", "HS256"],
    }
    doc.update(overrides)
    return {k: v for k, v in doc.items() if v is not None}


def _transport(
    body: object = None,
    *,
    status: int = 200,
    seen: list[httpx.Request] | None = None,
    raw: bytes | None = None,
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)

        if raw is not None:
            return httpx.Response(status, content=raw)

        return httpx.Response(status, json=_document() if body is None else body)

    return httpx.MockTransport(handler)


# ----------------------- #


async def test_fetches_well_known_and_returns_metadata() -> None:
    seen: list[httpx.Request] = []

    meta = await fetch_oidc_configuration(ISSUER, transport=_transport(seen=seen))

    assert str(seen[0].url) == f"{ISSUER}/.well-known/openid-configuration"
    assert meta == OidcProviderMetadata(
        issuer=ISSUER,
        jwks_uri=f"{ISSUER}/certs",
        introspection_endpoint=f"{ISSUER}/introspect",
        token_endpoint=f"{ISSUER}/token",
        userinfo_endpoint=f"{ISSUER}/userinfo",
    )


async def test_trailing_slash_issuer_is_compared_exactly() -> None:
    issuer = "https://idp.example/"
    seen: list[httpx.Request] = []

    meta = await fetch_oidc_configuration(
        issuer,
        transport=_transport(_document(issuer=issuer), seen=seen),
    )

    assert str(seen[0].url) == "https://idp.example/.well-known/openid-configuration"
    assert meta.issuer == issuer

    with pytest.raises(CoreException) as caught:
        await fetch_oidc_configuration(
            "https://idp.example", transport=_transport(_document(issuer=issuer))
        )

    assert caught.value.code == "oidc_issuer_mismatch"


@pytest.mark.parametrize(
    "doc_issuer",
    [
        "https://evil.example/realms/acme",
        f"{ISSUER}/",
        ISSUER.upper(),
        None,
        42,
    ],
)
async def test_issuer_mismatch_fails_closed(doc_issuer: object) -> None:
    body = _document()
    body["issuer"] = doc_issuer

    with pytest.raises(CoreException) as caught:
        await fetch_oidc_configuration(ISSUER, transport=_transport(body))

    assert caught.value.kind is ExceptionKind.CONFIGURATION
    assert caught.value.code == "oidc_issuer_mismatch"


@pytest.mark.parametrize(
    "issuer",
    [
        "http://idp.example",
        "ftp://idp.example",
        "idp.example",
        "https://",
        "https://idp.example?x=1",
        "https://idp.example#frag",
        "http://[::1",
    ],
)
async def test_insecure_or_malformed_issuer_refused_before_any_request(issuer: str) -> None:
    seen: list[httpx.Request] = []

    with pytest.raises(CoreException) as caught:
        await fetch_oidc_configuration(issuer, transport=_transport(seen=seen))

    assert caught.value.kind is ExceptionKind.CONFIGURATION
    assert seen == []


async def test_loopback_http_allowed_for_local_development() -> None:
    issuer = "http://127.0.0.1:8080/realms/dev"
    body = _document(
        issuer=issuer,
        jwks_uri=f"{issuer}/certs",
        introspection_endpoint=None,
        token_endpoint=None,
        userinfo_endpoint=None,
    )

    meta = await fetch_oidc_configuration(issuer, transport=_transport(body))

    assert meta.jwks_uri == f"{issuer}/certs"
    assert meta.introspection_endpoint is None


@pytest.mark.parametrize(
    "field", ["jwks_uri", "introspection_endpoint", "token_endpoint", "userinfo_endpoint"]
)
async def test_plaintext_endpoint_in_document_refused(field: str) -> None:
    with pytest.raises(CoreException) as caught:
        await fetch_oidc_configuration(
            ISSUER,
            transport=_transport(_document(**{field: "http://idp.example/x"})),
        )

    assert caught.value.kind is ExceptionKind.CONFIGURATION


@pytest.mark.parametrize(
    ("body", "status", "raw"),
    [
        pytest.param(_document(jwks_uri=None), 200, None, id="no-jwks-uri"),
        pytest.param(_document(jwks_uri=["x"]), 200, None, id="jwks-uri-not-string"),
        pytest.param(_document(token_endpoint=7), 200, None, id="endpoint-not-string"),
        pytest.param(["not", "an", "object"], 200, None, id="json-array"),
        pytest.param(None, 200, b"<html>", id="not-json"),
        pytest.param(None, 404, None, id="not-found"),
        pytest.param(None, 302, None, id="redirect-not-followed"),
        pytest.param(None, 503, None, id="server-error"),
    ],
)
async def test_bad_document_or_status_refused(body: object, status: int, raw: bytes | None) -> None:
    with pytest.raises(CoreException) as caught:
        await fetch_oidc_configuration(ISSUER, transport=_transport(body, status=status, raw=raw))

    assert caught.value.kind is ExceptionKind.CONFIGURATION


async def test_transport_failure_is_infrastructure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("boom", request=request)

    with pytest.raises(CoreException) as caught:
        await fetch_oidc_configuration(ISSUER, transport=httpx.MockTransport(handler))

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE
    assert caught.value.code == "oidc_discovery_unavailable"


# ....................... #


async def test_from_issuer_fills_preset_and_keeps_metadata() -> None:
    mapper = OidcClaimMapper(roles=ClaimsPath("realm_access.roles"))

    preset = await OidcIdpPreset.from_issuer(
        ISSUER,
        audience="orders-api",
        transport=_transport(),
        algorithms=("ES256",),
        claim_mapper=mapper,
    )

    assert preset.issuer == ISSUER
    assert preset.jwks_uri == f"{ISSUER}/certs"
    assert preset.audience == "orders-api"
    assert preset.metadata is not None
    assert preset.metadata.introspection_endpoint == f"{ISSUER}/introspect"
    # The document's alg list (which includes HS256) never widens the allowlist.
    assert tuple(preset.algorithms) == ("ES256",)

    verifier = ConfigurableOidcIdpVerifier(preset=preset)._verifier

    assert verifier.claim_mapper is mapper
    assert verifier.issuer == ISSUER


async def test_from_issuer_default_algorithms_ignore_document() -> None:
    preset = await OidcIdpPreset.from_issuer(ISSUER, audience="a", transport=_transport())

    assert tuple(preset.algorithms) == ("RS256",)


def test_preset_refuses_tenant_claim_beside_claim_mapper() -> None:
    with pytest.raises(CoreException, match="tenant_claim"):
        OidcIdpPreset(
            issuer=ISSUER,
            jwks_uri=f"{ISSUER}/certs",
            audience="a",
            tenant_claim="org",
            claim_mapper=OidcClaimMapper(),
        )


def test_preset_refuses_metadata_of_another_issuer() -> None:
    with pytest.raises(CoreException, match="different issuer"):
        OidcIdpPreset(
            issuer=ISSUER,
            jwks_uri=f"{ISSUER}/certs",
            audience="a",
            metadata=OidcProviderMetadata(issuer="https://other.example", jwks_uri="https://o/j"),
        )
