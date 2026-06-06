"""Tests for authn handler DTOs."""

from forze_kits.aggregates.authn import (
    AuthnChangePasswordRequestDTO,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    AuthnTokenResponseDTO,
)


class TestAuthnDTOs:
    def test_login_request(self) -> None:
        dto = AuthnLoginRequestDTO(login="alice", password="secret")
        assert dto.login == "alice"
        assert dto.password == "secret"

    def test_refresh_request(self) -> None:
        dto = AuthnRefreshRequestDTO(refresh_token="rtok")
        assert dto.refresh_token == "rtok"

    def test_change_password_request(self) -> None:
        dto = AuthnChangePasswordRequestDTO(
            current_password="old-secret",
            new_password="new-secret",
        )
        assert dto.current_password == "old-secret"
        assert dto.new_password == "new-secret"

    def test_token_response_defaults(self) -> None:
        dto = AuthnTokenResponseDTO()
        assert dto.access_token is None
        assert dto.access_token_type == "Bearer"

    def test_token_response_full(self) -> None:
        dto = AuthnTokenResponseDTO(
            access_token="a",
            refresh_token="r",
            access_expires_in=60,
            refresh_expires_in=120,
        )
        assert dto.access_token == "a"
        assert dto.refresh_expires_in == 120
