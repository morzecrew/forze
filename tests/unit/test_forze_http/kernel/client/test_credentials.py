"""Tests for :func:`forze_http.kernel.client.credentials.credential_auth_headers`."""

from pydantic import SecretStr

from forze_http.kernel.client.credentials import credential_auth_headers
from forze_http.kernel.client.routing_credentials import HttpRoutingCredentials

# ----------------------- #


def test_bearer_added_when_no_authorization_header() -> None:
    creds = HttpRoutingCredentials(base_url="https://x", bearer_token=SecretStr("tok"))
    headers = credential_auth_headers(creds)
    assert headers["Authorization"] == "Bearer tok"


def test_lowercase_authorization_header_suppresses_default_bearer() -> None:
    # HTTP header names are case-insensitive: an explicit ``authorization``
    # header (any casing) must prevent a second, conflicting Authorization.
    creds = HttpRoutingCredentials(
        base_url="https://x",
        headers={"authorization": "Bearer explicit"},
        bearer_token=SecretStr("default"),
    )
    headers = credential_auth_headers(creds)

    auth_keys = [k for k in headers if k.lower() == "authorization"]
    assert auth_keys == ["authorization"]
    assert headers["authorization"] == "Bearer explicit"
    assert "default" not in str(headers)


def test_explicit_titlecase_authorization_still_suppresses_default() -> None:
    creds = HttpRoutingCredentials(
        base_url="https://x",
        headers={"Authorization": "Bearer explicit"},
        bearer_token=SecretStr("default"),
    )
    headers = credential_auth_headers(creds)
    assert headers["Authorization"] == "Bearer explicit"
