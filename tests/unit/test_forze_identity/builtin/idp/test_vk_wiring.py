"""VK ID bootstrap wiring tests."""

from __future__ import annotations

import pytest

from forze.application.contracts.authn import AuthnDepKey
from forze_identity.builtin.idp.vk import VkIdOidcConfig, vk_identity_deps

pytestmark = pytest.mark.unit


def test_vk_identity_deps_registers_bootstrap_route() -> None:
    config = VkIdOidcConfig(client_id="vk", redirect_uri="https://app/cb")
    deps = vk_identity_deps(config)

    assert deps.exists(AuthnDepKey, route="bootstrap")
