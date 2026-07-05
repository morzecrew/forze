"""Telegram Login Widget HMAC verifier tests."""

from __future__ import annotations

import hashlib
import hmac
from datetime import timedelta
from urllib.parse import urlencode

import pytest
from pydantic import SecretStr

from forze.application.contracts.authn import AccessTokenCredentials
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze_identity.builtin.idp.telegram import (
    TELEGRAM_LOGIN_WIDGET_ISSUER,
    TelegramWidgetVerifier,
)

pytestmark = pytest.mark.unit

_BOT_TOKEN = "123456:test-bot-token"


def _sign(bot_token: str, fields: dict[str, str]) -> dict[str, str]:
    """Return *fields* plus the Telegram-computed ``hash`` (the signed widget payload)."""

    data_check_string = "\n".join(f"{k}={fields[k]}" for k in sorted(fields))
    secret_key = hashlib.sha256(bot_token.encode()).digest()
    digest = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    return {**fields, "hash": digest}


def _fresh_fields(**overrides: str) -> dict[str, str]:
    base = {
        "id": "42",
        "first_name": "Alice",
        "username": "alice",
        "auth_date": str(int(utcnow().timestamp())),
    }
    base.update(overrides)
    return base


# ....................... #


def test_valid_widget_payload_passes() -> None:
    verifier = TelegramWidgetVerifier(bot_token=_BOT_TOKEN, bot_username="mybot")
    payload = _sign(_BOT_TOKEN, _fresh_fields())

    assertion = verifier.verify(payload)

    assert assertion.issuer == TELEGRAM_LOGIN_WIDGET_ISSUER
    assert assertion.subject == "42"
    assert assertion.audience == "mybot"
    assert assertion.claims["username"] == "alice"
    assert "hash" not in assertion.claims  # the authenticator is not echoed as a claim


def test_tampered_field_is_rejected() -> None:
    verifier = TelegramWidgetVerifier(bot_token=_BOT_TOKEN)
    payload = _sign(_BOT_TOKEN, _fresh_fields())
    payload["id"] = "999"  # change a signed field after signing

    with pytest.raises(CoreException) as ei:
        verifier.verify(payload)
    assert ei.value.code == "telegram_widget_invalid"


def test_wrong_bot_token_is_rejected() -> None:
    verifier = TelegramWidgetVerifier(bot_token="other:token")
    payload = _sign(_BOT_TOKEN, _fresh_fields())

    with pytest.raises(CoreException) as ei:
        verifier.verify(payload)
    assert ei.value.code == "telegram_widget_invalid"


def test_missing_hash_is_rejected() -> None:
    verifier = TelegramWidgetVerifier(bot_token=_BOT_TOKEN)

    with pytest.raises(CoreException) as ei:
        verifier.verify(_fresh_fields())  # no hash
    assert ei.value.code == "telegram_widget_invalid"


def test_stale_auth_date_is_rejected() -> None:
    verifier = TelegramWidgetVerifier(bot_token=_BOT_TOKEN, max_age=timedelta(hours=1))
    old = str(int((utcnow() - timedelta(hours=2)).timestamp()))
    payload = _sign(_BOT_TOKEN, _fresh_fields(auth_date=old))

    with pytest.raises(CoreException) as ei:
        verifier.verify(payload)
    assert ei.value.code == "telegram_widget_expired"


def test_max_age_none_skips_freshness() -> None:
    verifier = TelegramWidgetVerifier(bot_token=_BOT_TOKEN, max_age=None)
    old = str(int((utcnow() - timedelta(days=365)).timestamp()))
    payload = _sign(_BOT_TOKEN, _fresh_fields(auth_date=old))

    assert verifier.verify(payload).subject == "42"


def test_missing_id_is_rejected() -> None:
    verifier = TelegramWidgetVerifier(bot_token=_BOT_TOKEN)
    # A validly-signed payload that simply carries no user id.
    fields = {
        "first_name": "Nobody",
        "auth_date": str(int(utcnow().timestamp())),
    }
    payload = _sign(_BOT_TOKEN, fields)

    with pytest.raises(CoreException) as ei:
        verifier.verify(payload)
    assert ei.value.code == "telegram_widget_invalid"


@pytest.mark.asyncio
async def test_verify_token_parses_query_string() -> None:
    verifier = TelegramWidgetVerifier(bot_token=_BOT_TOKEN)
    payload = _sign(_BOT_TOKEN, _fresh_fields())
    token = urlencode(payload)

    assertion = await verifier.verify_token(AccessTokenCredentials(token=token))

    assert assertion.subject == "42"


def test_secret_str_bot_token_works_and_is_hidden() -> None:
    verifier = TelegramWidgetVerifier(bot_token=SecretStr(_BOT_TOKEN))
    payload = _sign(_BOT_TOKEN, _fresh_fields())

    assert verifier.verify(payload).subject == "42"
    assert _BOT_TOKEN not in repr(verifier)  # repr must not leak the token


def test_empty_bot_token_rejected() -> None:
    with pytest.raises(CoreException):
        TelegramWidgetVerifier(bot_token="   ")
