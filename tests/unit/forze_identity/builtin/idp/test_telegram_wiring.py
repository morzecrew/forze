"""Telegram Login bootstrap wiring tests."""

from __future__ import annotations

import pytest

from forze.application.contracts.authn import AuthnDepKey
from forze_identity.builtin.idp.telegram import (
    TelegramLoginOidcConfig,
    telegram_login_identity_deps,
)

pytestmark = pytest.mark.unit


def test_telegram_login_identity_deps_registers_bootstrap_route() -> None:
    config = TelegramLoginOidcConfig(
        client_id="bot",
        client_secret="secret",
        redirect_uri="https://app/cb",
    )
    deps = telegram_login_identity_deps(config)

    assert deps.exists(AuthnDepKey, route="bootstrap")
