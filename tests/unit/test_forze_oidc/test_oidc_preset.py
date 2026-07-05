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


def _spec() -> AuthnSpec:
    return AuthnSpec(name="api", enabled_methods=frozenset({"token"}))


def test_configurable_oidc_idp_verifier_builds_token_verifier() -> None:
    preset = OidcIdpPreset(
        issuer="https://issuer.example",
        jwks_uri="https://issuer.example/jwks",
        audience="app",
    )
    factory = ConfigurableOidcIdpVerifier(preset=preset)

    verifier = factory(MagicMock(), _spec())

    assert isinstance(verifier, OidcTokenVerifier)


def test_verifier_and_jwks_provider_built_once_and_reused() -> None:
    """The verifier (and its JWKS key provider) must be reused across calls, so the
    JWKS cache spans requests instead of re-fetching per verification (amplifier)."""

    preset = OidcIdpPreset(
        issuer="https://issuer.example",
        jwks_uri="https://issuer.example/jwks",
        audience="app",
    )
    factory = ConfigurableOidcIdpVerifier(preset=preset)

    first = factory(MagicMock(), _spec())
    second = factory(MagicMock(), _spec())

    assert first is second  # same verifier instance
    assert first.key_provider is second.key_provider  # same (caching) JWKS provider


def test_require_nonce_plumbed_from_preset() -> None:
    preset = OidcIdpPreset(
        issuer="https://issuer.example",
        jwks_uri="https://issuer.example/jwks",
        audience="app",
        require_nonce=True,
    )
    verifier = ConfigurableOidcIdpVerifier(preset=preset)(MagicMock(), _spec())

    assert isinstance(verifier, OidcTokenVerifier)
    assert verifier.require_nonce is True


def test_require_nonce_defaults_false() -> None:
    preset = OidcIdpPreset(
        issuer="https://issuer.example",
        jwks_uri="https://issuer.example/jwks",
        audience="app",
    )
    verifier = ConfigurableOidcIdpVerifier(preset=preset)(MagicMock(), _spec())

    assert verifier.require_nonce is False
