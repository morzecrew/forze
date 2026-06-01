"""Tests for :mod:`forze.application.handlers.authn._utils`."""

from datetime import timedelta

from forze.application.contracts.authn import (
    IssuedAccessToken,
    IssuedRefreshToken,
    IssuedTokens,
)
from forze.application.contracts.authn.value_objects import (
    AccessTokenCredentials,
    CredentialLifetime,
    RefreshTokenCredentials,
)
from forze.application.handlers.authn._utils import token_response_from_issued_tokens
from forze.application.handlers.authn.dto import AuthnTokenResponseDTO


class TestTokenResponseFromIssuedTokens:
    def test_maps_access_and_refresh_seconds(self) -> None:
        tokens = IssuedTokens(
            access=IssuedAccessToken(
                token=AccessTokenCredentials(token="acc", scheme="Bearer"),
                lifetime=CredentialLifetime(expires_in=timedelta(seconds=90)),
            ),
            refresh=IssuedRefreshToken(
                token=RefreshTokenCredentials(token="ref"),
                lifetime=CredentialLifetime(expires_in=timedelta(seconds=3600)),
            ),
        )

        dto = token_response_from_issued_tokens(tokens)

        assert isinstance(dto, AuthnTokenResponseDTO)
        assert dto.access_token == "acc"
        assert dto.access_token_type == "Bearer"
        assert dto.access_expires_in == 90
        assert dto.refresh_token == "ref"
        assert dto.refresh_expires_in == 3600

    def test_access_only_when_no_refresh(self) -> None:
        tokens = IssuedTokens(
            access=IssuedAccessToken(
                token=AccessTokenCredentials(token="only"),
                lifetime=None,
            ),
            refresh=None,
        )

        dto = token_response_from_issued_tokens(tokens)

        assert dto.access_token == "only"
        assert dto.access_expires_in is None
        assert dto.refresh_token is None
        assert dto.refresh_expires_in is None
