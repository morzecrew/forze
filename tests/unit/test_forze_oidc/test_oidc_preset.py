"""Tests for :class:`OidcIdpPreset` and :class:`ConfigurableOidcIdpVerifier`."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forze.application.contracts.authn import AuthnSpec
from forze_identity.oidc import (
    ConfigurableOidcIdpVerifier,
    OidcIdpPreset,
    OidcTokenVerifier,
)

pytestmark = pytest.mark.unit


def test_configurable_oidc_idp_verifier_builds_token_verifier() -> None:
    preset = OidcIdpPreset(
        issuer="https://issuer.example",
        jwks_uri="https://issuer.example/jwks",
        audience="app",
    )
    factory = ConfigurableOidcIdpVerifier(preset=preset)

    verifier = factory(
        MagicMock(),
        AuthnSpec(name="api", enabled_methods=frozenset({"token"})),
    )

    assert isinstance(verifier, OidcTokenVerifier)
