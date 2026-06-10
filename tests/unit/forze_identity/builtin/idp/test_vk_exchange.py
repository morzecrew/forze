"""VK ID authorization-code exchange tests."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from forze.base.exceptions import CoreException
from forze_identity.builtin.idp.vk import (
    VK_ID_OIDC_ISSUER,
    VkIdOidcConfig,
    exchange_authorization_code,
)

pytestmark = pytest.mark.unit


def _mock_httpx_client(monkeypatch: pytest.MonkeyPatch, *, status: int, json_body: Any) -> list[dict[str, Any]]:
    captured: list[dict[str, Any]] = []

    class _FakeResponse:
        status_code = status

        def json(self) -> Any:
            return json_body

    class _FakeClient:
        def __init__(self, **kwargs: Any) -> None:
            _ = kwargs

        async def __aenter__(self) -> _FakeClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            _ = args

        async def post(self, url: str, **kwargs: Any) -> _FakeResponse:
            captured.append({"url": url, **kwargs})
            return _FakeResponse()

    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    return captured


@pytest.mark.asyncio
async def test_exchange_authorization_code_success(monkeypatch: pytest.MonkeyPatch) -> None:
    config = VkIdOidcConfig(
        client_id="vk-app",
        redirect_uri="https://app.example/callback",
    )
    captured = _mock_httpx_client(
        monkeypatch,
        status=200,
        json_body={
            "id_token": "eyJ.test",
            "access_token": "opaque-vk",
            "refresh_token": "refresh",
            "expires_in": 3600,
            "token_type": "Bearer",
        },
    )

    result = await exchange_authorization_code(
        config,
        code="auth-code",
        code_verifier="verifier",
        device_id="device-1",
    )

    assert result.id_token == "eyJ.test"
    assert result.access_token == "opaque-vk"
    assert result.refresh_token == "refresh"
    assert result.expires_in == 3600
    assert captured[0]["url"] == config.token_endpoint
    assert captured[0]["data"]["device_id"] == "device-1"


@pytest.mark.asyncio
async def test_exchange_rejects_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    config = VkIdOidcConfig(client_id="vk-app", redirect_uri="https://app.example/cb")
    _mock_httpx_client(monkeypatch, status=400, json_body={"error": "invalid_grant"})

    with pytest.raises(CoreException) as ei:
        await exchange_authorization_code(
            config,
            code="bad",
            code_verifier="v",
        )
    assert ei.value.code == "vk_token_exchange_failed"


@pytest.mark.asyncio
async def test_exchange_missing_id_token(monkeypatch: pytest.MonkeyPatch) -> None:
    config = VkIdOidcConfig(client_id="vk-app", redirect_uri="https://app.example/cb")
    _mock_httpx_client(monkeypatch, status=200, json_body={"access_token": "only"})

    with pytest.raises(CoreException) as ei:
        await exchange_authorization_code(config, code="c", code_verifier="v")
    assert ei.value.code == "vk_token_exchange_failed"


def test_vk_config_defaults() -> None:
    config = VkIdOidcConfig(client_id="1", redirect_uri="https://x/cb")

    assert config.issuer == VK_ID_OIDC_ISSUER
    assert config.token_endpoint == "https://id.vk.ru/oauth2/auth"
    assert config.public_info_endpoint == "https://id.vk.ru/oauth2/public_info"
