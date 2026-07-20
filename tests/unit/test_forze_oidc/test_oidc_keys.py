"""Unit tests for OIDC signing key providers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("jwt")

from jwt import InvalidTokenError, PyJWKClientError

from forze.base.exceptions import CoreException
from forze_identity.oidc.keys import JwksKeyProvider, StaticKeyProvider

# ----------------------- #


class TestStaticKeyProvider:
    def test_returns_configured_key(self) -> None:
        provider = StaticKeyProvider(key=b"secret")
        assert provider.get_signing_key("any.jwt.token") == b"secret"


class TestJwksKeyProvider:
    def test_lazy_client_and_resolves_key(self) -> None:
        provider = JwksKeyProvider(jwks_uri="https://issuer.example/jwks")
        mock_client = MagicMock()
        signing = MagicMock()
        signing.key = b"rsa-key"
        mock_client.get_signing_key_from_jwt.return_value = signing

        with patch.object(provider, "_require_client", return_value=mock_client):
            assert provider.get_signing_key("header.payload.sig") == b"rsa-key"

        mock_client.get_signing_key_from_jwt.assert_called_once_with("header.payload.sig")

    def test_maps_jwks_client_error_to_authentication(self) -> None:
        provider = JwksKeyProvider(jwks_uri="https://issuer.example/jwks")
        mock_client = MagicMock()
        mock_client.get_signing_key_from_jwt.side_effect = PyJWKClientError("network")

        with patch.object(provider, "_require_client", return_value=mock_client):
            with pytest.raises(CoreException, match="signing key") as exc_info:
                provider.get_signing_key("bad")

        assert exc_info.value.code == "invalid_oidc_signing_key"

    def test_maps_invalid_token_to_authentication(self) -> None:
        provider = JwksKeyProvider(jwks_uri="https://issuer.example/jwks")
        mock_client = MagicMock()
        mock_client.get_signing_key_from_jwt.side_effect = InvalidTokenError("malformed")

        with patch.object(provider, "_require_client", return_value=mock_client):
            with pytest.raises(CoreException, match="signing key"):
                provider.get_signing_key("not-a-jwt")
