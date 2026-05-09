"""Tests for OpenAPI security helpers."""

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from forze_fastapi.openapi.security import (
    extract_bearer_token_or_raise,
    openapi_api_key_cookie_scheme,
    openapi_http_bearer_scheme,
    openapi_operation_security,
)


@pytest.mark.unit
def test_openapi_http_bearer_scheme_shape() -> None:
    s = openapi_http_bearer_scheme()

    assert s["httpBearer"]["type"] == "http"
    assert s["httpBearer"]["scheme"] == "bearer"


@pytest.mark.unit
def test_openapi_cookie_scheme_shape() -> None:
    s = openapi_api_key_cookie_scheme(scheme_name="sid", cookie_name="session")

    assert s["sid"]["in"] == "cookie"


@pytest.mark.unit
def test_extract_bearer_token_or_raise_ok() -> None:
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="abc")

    assert extract_bearer_token_or_raise(creds) == "abc"


@pytest.mark.unit
def test_openapi_operation_security_single_scheme() -> None:
    extra = openapi_operation_security("httpBearer")

    assert extra == {"security": [{"httpBearer": []}]}


@pytest.mark.unit
def test_openapi_operation_security_and_of_two() -> None:
    extra = openapi_operation_security("httpBearer", "apiKey")

    assert extra == {"security": [{"httpBearer": [], "apiKey": []}]}


@pytest.mark.unit
def test_openapi_operation_security_requires_name() -> None:
    with pytest.raises(ValueError, match="At least one"):
        openapi_operation_security()


@pytest.mark.unit
def test_extract_bearer_token_or_raise_missing() -> None:
    with pytest.raises(HTTPException) as ei:
        extract_bearer_token_or_raise(None)

    assert ei.value.status_code == 401
