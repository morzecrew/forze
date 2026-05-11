"""OpenAPI / FastAPI security helpers for Bearer and optional cookie-style APIs."""

from typing import Any

from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

# ----------------------- #


def http_bearer_scheme(*, auto_error: bool = False) -> HTTPBearer:
    """Return a shared :class:`~fastapi.security.HTTPBearer` dependency factory."""

    return HTTPBearer(auto_error=auto_error)


# ....................... #


def openapi_http_bearer_scheme(
    *,
    scheme_name: str = "httpBearer",
    bearer_format: str = "JWT",
    description: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Return a single-entry ``components.securitySchemes`` fragment for HTTP bearer."""

    body: dict[str, Any] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": bearer_format,
    }

    if description is not None:
        body["description"] = description

    return {scheme_name: body}


# ....................... #


def openapi_api_key_cookie_scheme(
    *,
    scheme_name: str,
    cookie_name: str,
    description: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Return a ``components.securitySchemes`` fragment for an API key in a cookie."""

    body: dict[str, Any] = {
        "type": "apiKey",
        "in": "cookie",
        "name": cookie_name,
    }

    if description is not None:
        body["description"] = description

    return {scheme_name: body}


# ....................... #


def openapi_api_key_header_scheme(
    *,
    scheme_name: str,
    header_name: str,
    description: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Return a ``components.securitySchemes`` fragment for an API key in a header."""

    body: dict[str, Any] = {
        "type": "apiKey",
        "in": "header",
        "name": header_name,
    }

    if description is not None:
        body["description"] = description

    return {scheme_name: body}


# ....................... #


def openapi_operation_security(
    *scheme_names: str,
) -> dict[str, Any]:
    """Return an ``openapi_extra`` fragment with a single ``security`` requirement (AND of schemes).

    Use scheme names that match keys under ``components.securitySchemes`` (for example from
    :func:`openapi_http_bearer_scheme`). For OAuth2-style alternatives, call this helper
    per alternative and merge operations manually.

    :param scheme_names: One or more security scheme names required together on the operation.
    """

    if not scheme_names:
        raise ValueError("At least one security scheme name is required")

    return {"security": [{name: [] for name in scheme_names}]}


# ....................... #


def extract_bearer_token_or_raise(
    creds: HTTPAuthorizationCredentials | None,
) -> str:
    """Validate ``HTTPBearer`` credentials and return the token string."""

    if (
        creds is None
        or creds.scheme.lower() != "bearer"
        or not creds.credentials.strip()
    ):
        raise HTTPException(
            status_code=401,
            detail="Missing or invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return creds.credentials.strip()
