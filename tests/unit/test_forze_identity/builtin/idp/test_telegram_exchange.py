"""Telegram Login OIDC exchange tests."""

from __future__ import annotations

from typing import Any

import httpx
import jwt
import pytest

from forze.base.exceptions import CoreException
from forze_identity.builtin.idp.telegram import (
    TELEGRAM_LOGIN_OIDC_ISSUER,
    TelegramLoginOidcConfig,
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
    config = TelegramLoginOidcConfig(
        client_id="bot-id",
        client_secret="secret",
        redirect_uri="https://app.example/callback",
    )
    captured = _mock_httpx_client(
        monkeypatch,
        status=200,
        json_body={"id_token": "eyJ.tg", "access_token": "opaque", "expires_in": 3600},
    )

    result = await exchange_authorization_code(
        config,
        code="code",
        code_verifier="verifier",
    )

    assert result.id_token == "eyJ.tg"
    assert captured[0]["url"] == config.token_endpoint
    headers = captured[0]["headers"]
    assert headers["Authorization"].startswith("Basic ")


@pytest.mark.asyncio
async def test_exchange_rejects_missing_id_token(monkeypatch: pytest.MonkeyPatch) -> None:
    config = TelegramLoginOidcConfig(
        client_id="bot",
        client_secret="s",
        redirect_uri="https://x/cb",
    )
    _mock_httpx_client(monkeypatch, status=200, json_body={})

    with pytest.raises(CoreException) as ei:
        await exchange_authorization_code(config, code="c", code_verifier="v")
    assert ei.value.code == "telegram_token_exchange_failed"


@pytest.mark.asyncio
async def test_exchange_binds_expected_nonce(monkeypatch: pytest.MonkeyPatch) -> None:
    config = TelegramLoginOidcConfig(
        client_id="bot",
        client_secret="s",
        redirect_uri="https://x/cb",
    )
    id_token = jwt.encode({"sub": "u", "nonce": "n-1"}, "k" * 32, algorithm="HS256")
    _mock_httpx_client(monkeypatch, status=200, json_body={"id_token": id_token})

    result = await exchange_authorization_code(
        config,
        code="c",
        code_verifier="v",
        expected_nonce="n-1",
    )

    assert result.id_token == id_token


@pytest.mark.asyncio
async def test_exchange_rejects_nonce_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    config = TelegramLoginOidcConfig(
        client_id="bot",
        client_secret="s",
        redirect_uri="https://x/cb",
    )
    id_token = jwt.encode({"sub": "u"}, "k" * 32, algorithm="HS256")  # nonce absent
    _mock_httpx_client(monkeypatch, status=200, json_body={"id_token": id_token})

    with pytest.raises(CoreException) as ei:
        await exchange_authorization_code(
            config,
            code="c",
            code_verifier="v",
            expected_nonce="n-1",
        )
    assert ei.value.code == "oidc_nonce_mismatch"


def test_telegram_preset_defaults() -> None:
    config = TelegramLoginOidcConfig(
        client_id="bot",
        client_secret="sec",
        redirect_uri="https://x/cb",
    )
    preset = config.to_preset()

    assert preset.issuer == TELEGRAM_LOGIN_OIDC_ISSUER
    assert preset.audience == "bot"
