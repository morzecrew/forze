"""VK ID ``public_info`` introspection verifier tests."""

from __future__ import annotations

from typing import cast

import httpx
import pytest

from forze.application.contracts.authn import AccessTokenCredentials, AuthnSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.builtin.idp.vk import (
    VK_ID_OIDC_ISSUER,
    VK_ID_PUBLIC_INFO_ENDPOINT,
    ConfigurableVkIdOidcVerifier,
    VkIdOidcConfig,
    VkPublicInfoTokenVerifier,
)

pytestmark = pytest.mark.unit

# ----------------------- #


def _verifier(
    handler: httpx.MockTransport | None = None,
    **overrides: object,
) -> VkPublicInfoTokenVerifier:
    kwargs: dict[str, object] = {"client_id": "vk-app", "transport": handler}
    kwargs.update(overrides)
    return VkPublicInfoTokenVerifier(**kwargs)  # type: ignore[arg-type]


def _json_transport(
    payload: object,
    *,
    status: int = 200,
    captured: list[httpx.Request] | None = None,
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if captured is not None:
            captured.append(request)
        return httpx.Response(status, json=payload)

    return httpx.MockTransport(handler)


# ----------------------- #


@pytest.mark.asyncio
async def test_success_emits_assertion_with_issuer_and_subject() -> None:
    captured: list[httpx.Request] = []
    transport = _json_transport(
        {
            "user": {
                "user_id": "123456",
                "first_name": "Ivan",
                "last_name": "I",
                "avatar": "https://example/avatar.jpg",
            }
        },
        captured=captured,
    )

    assertion = await _verifier(transport).verify_token(
        AccessTokenCredentials(token="eyJ.id.token"),
    )

    assert assertion.issuer == VK_ID_OIDC_ISSUER
    assert assertion.subject == "123456"
    assert assertion.audience == "vk-app"
    assert assertion.claims["user"]["first_name"] == "Ivan"

    # Request shape: form-encoded POST with client_id + id_token, to the default URL.
    request = captured[0]
    assert str(request.url) == VK_ID_PUBLIC_INFO_ENDPOINT
    assert request.headers["content-type"] == "application/x-www-form-urlencoded"
    body = request.content.decode()
    assert "client_id=vk-app" in body
    assert "id_token=eyJ.id.token" in body


@pytest.mark.asyncio
async def test_success_with_integer_or_top_level_user_id() -> None:
    transport = _json_transport({"user": {"user_id": 42}})
    assertion = await _verifier(transport).verify_token(
        AccessTokenCredentials(token="t"),
    )
    assert assertion.subject == "42"

    transport = _json_transport({"user_id": "77"})
    assertion = await _verifier(transport).verify_token(
        AccessTokenCredentials(token="t"),
    )
    assert assertion.subject == "77"


@pytest.mark.asyncio
async def test_vendor_invalid_id_token_maps_to_authentication() -> None:
    # VK replies HTTP 200 with an error payload (observed live 2026-06-10).
    transport = _json_transport(
        {"error": "invalid_id_token", "error_description": "can't validate id_token"},
    )

    with pytest.raises(CoreException) as ei:
        await _verifier(transport).verify_token(AccessTokenCredentials(token="bad"))

    assert ei.value.kind is ExceptionKind.AUTHENTICATION
    assert ei.value.code == "vk_id_token_rejected"
    assert ei.value.details == {"vendor_error": "invalid_id_token"}
    # Safe generic message — never echo token contents.
    assert "bad" not in ei.value.summary


@pytest.mark.asyncio
async def test_vendor_invalid_client_maps_to_authentication() -> None:
    # Deliberately authentication, not configuration: the verifier cannot
    # distinguish "our client_id is misconfigured" from "this token was minted
    # for another app", and a remote payload must not reclassify local config.
    transport = _json_transport(
        {"error": "invalid_request", "error_description": "client_id is invalid"},
    )

    with pytest.raises(CoreException) as ei:
        await _verifier(transport).verify_token(AccessTokenCredentials(token="t"))

    assert ei.value.kind is ExceptionKind.AUTHENTICATION
    assert ei.value.code == "vk_id_token_rejected"


@pytest.mark.asyncio
async def test_non_json_404_maps_to_infrastructure() -> None:
    # The old JWKS failure mode: an HTML 404 page. Must be a clean error.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="<html><body>404</body></html>")

    with pytest.raises(CoreException) as ei:
        await _verifier(httpx.MockTransport(handler)).verify_token(
            AccessTokenCredentials(token="t"),
        )

    assert ei.value.kind is ExceptionKind.INFRASTRUCTURE
    assert ei.value.code == "vk_public_info_invalid_response"


@pytest.mark.asyncio
async def test_http_error_status_with_json_body_maps_to_authentication() -> None:
    transport = _json_transport({"unexpected": "shape"}, status=403)

    with pytest.raises(CoreException) as ei:
        await _verifier(transport).verify_token(AccessTokenCredentials(token="t"))

    assert ei.value.kind is ExceptionKind.AUTHENTICATION


@pytest.mark.asyncio
async def test_non_object_json_maps_to_infrastructure() -> None:
    transport = _json_transport(["not", "an", "object"])

    with pytest.raises(CoreException) as ei:
        await _verifier(transport).verify_token(AccessTokenCredentials(token="t"))

    assert ei.value.kind is ExceptionKind.INFRASTRUCTURE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"user": {}},
        {"user": {"user_id": ""}},
        {"user": {"user_id": "   "}},
        {"user": {"user_id": True}},
        {"user": {"user_id": None}},
        {"user": {"user_id": ["1"]}},
        {"user": "not-a-dict"},
    ],
)
async def test_success_payload_without_user_id_is_rejected(payload: object) -> None:
    transport = _json_transport(payload)

    with pytest.raises(CoreException) as ei:
        await _verifier(transport).verify_token(AccessTokenCredentials(token="t"))

    assert ei.value.kind is ExceptionKind.AUTHENTICATION
    assert ei.value.code == "vk_id_token_rejected"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [httpx.ConnectTimeout("timed out"), httpx.ConnectError("refused")],
)
async def test_transport_errors_map_to_infrastructure(error: httpx.HTTPError) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error

    with pytest.raises(CoreException) as ei:
        await _verifier(httpx.MockTransport(handler)).verify_token(
            AccessTokenCredentials(token="t"),
        )

    assert ei.value.kind is ExceptionKind.INFRASTRUCTURE
    assert ei.value.code == "vk_public_info_unavailable"


@pytest.mark.asyncio
async def test_configurable_factory_builds_introspection_verifier() -> None:
    config = VkIdOidcConfig(
        client_id="vk-app",
        redirect_uri="https://app/cb",
        public_info_endpoint="https://id.vk.ru/oauth2/public_info",
        verify_timeout=5.0,
    )
    factory = ConfigurableVkIdOidcVerifier(config=config)

    verifier = factory(
        cast(ExecutionContext, None),
        cast(AuthnSpec, None),
    )

    assert isinstance(verifier, VkPublicInfoTokenVerifier)
    assert verifier.client_id == "vk-app"
    assert verifier.public_info_url == config.public_info_endpoint
    assert verifier.issuer == config.issuer
    assert verifier.timeout == 5.0


def test_verifier_validates_construction() -> None:
    with pytest.raises(CoreException) as ei:
        VkPublicInfoTokenVerifier(client_id="  ")
    assert ei.value.kind is ExceptionKind.CONFIGURATION

    with pytest.raises(CoreException) as ei:
        VkPublicInfoTokenVerifier(client_id="vk", timeout=0)
    assert ei.value.kind is ExceptionKind.CONFIGURATION
