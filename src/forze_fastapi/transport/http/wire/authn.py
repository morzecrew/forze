"""Authn token transport read/write helpers (rewritten from legacy features)."""

from typing import Any, Literal, TypedDict

from fastapi import HTTPException, Response
from fastapi.responses import JSONResponse

from forze.application.handlers.authn import (
    AuthnTokenResponseDTO,
)

# ----------------------- #

CookieSameSite = Literal["lax", "strict", "none"]


class HeaderTokenTransportSpec(TypedDict, total=False):
    kind: Literal["header"]
    header_name: str
    scheme: str


class CookieTokenTransportSpec(TypedDict, total=False):
    kind: Literal["cookie"]
    cookie_name: str
    cookie_secure: bool
    cookie_http_only: bool
    cookie_samesite: CookieSameSite
    cookie_path: str
    cookie_domain: str | None
    cookie_max_age_from_lifetime: bool


TokenTransportSpec = HeaderTokenTransportSpec | CookieTokenTransportSpec
TokenTransportMode = Literal["issue", "clear"]


def default_access_transport() -> HeaderTokenTransportSpec:
    return HeaderTokenTransportSpec(
        kind="header",
        header_name="Authorization",
        scheme="Bearer",
    )


def default_refresh_transport() -> HeaderTokenTransportSpec:
    return HeaderTokenTransportSpec(
        kind="header",
        header_name="X-Refresh-Token",
        scheme="",
    )


def _is_cookie(spec: TokenTransportSpec) -> bool:
    return spec.get("kind") == "cookie"


def _set_token_cookie(
    response: Response,
    *,
    spec: CookieTokenTransportSpec,
    token: str,
    expires_in: int | None,
) -> None:
    cookie_name = spec.get("cookie_name")
    if not cookie_name:
        raise HTTPException(status_code=500, detail="cookie_name is required for cookie transport")

    max_age: int | None = None
    if spec.get("cookie_max_age_from_lifetime", True):
        max_age = expires_in

    response.set_cookie(
        key=cookie_name,
        value=token,
        max_age=max_age,
        path=spec.get("cookie_path", "/"),
        domain=spec.get("cookie_domain"),
        secure=spec.get("cookie_secure", True),
        httponly=spec.get("cookie_http_only", True),
        samesite=spec.get("cookie_samesite", "lax"),
    )


def _delete_token_cookie(response: Response, *, spec: CookieTokenTransportSpec) -> None:
    cookie_name = spec.get("cookie_name")
    if not cookie_name:
        return
    response.delete_cookie(
        key=cookie_name,
        path=spec.get("cookie_path", "/"),
        domain=spec.get("cookie_domain"),
        secure=spec.get("cookie_secure", True),
        httponly=spec.get("cookie_http_only", True),
        samesite=spec.get("cookie_samesite", "lax"),
    )


def _build_json_response(payload: AuthnTokenResponseDTO) -> Response:
    return JSONResponse(content=payload.model_dump(mode="json", exclude_none=True))


def _strip_for_cookie(payload: AuthnTokenResponseDTO, *, field: str) -> AuthnTokenResponseDTO:
    return payload.model_copy(update={field: None})


def extract_refresh_token(request: Any, spec: TokenTransportSpec) -> str | None:
    spec_dict: dict[str, Any] = dict(spec)

    if _is_cookie(spec):
        cookie_name = spec_dict.get("cookie_name")
        if not cookie_name:
            return None
        raw = request.cookies.get(str(cookie_name))
        return str(raw).strip() if raw else None

    if spec_dict.get("kind") == "header":
        header_name = spec_dict.get("header_name")
        if not header_name:
            return None
        raw = request.headers.get(str(header_name))
        if not raw:
            return None
        scheme_hint = spec_dict.get("scheme")
        if scheme_hint and raw.lower().startswith(str(scheme_hint).lower() + " "):
            return raw[len(str(scheme_hint)) + 1 :].strip()
        parts = raw.strip().split(maxsplit=1)
        return parts[1].strip() if len(parts) == 2 else parts[0].strip()

    return None


def apply_token_transport_output(
    payload: AuthnTokenResponseDTO | None,
    *,
    access_transport: TokenTransportSpec,
    refresh_transport: TokenTransportSpec | None,
    mode: TokenTransportMode,
    inner_response: Response | None = None,
) -> Response:
    if mode == "clear":
        response = inner_response or Response(status_code=204)
        if _is_cookie(access_transport):
            _delete_token_cookie(response, spec=access_transport)  # type: ignore[arg-type]
        if refresh_transport is not None and _is_cookie(refresh_transport):
            _delete_token_cookie(response, spec=refresh_transport)  # type: ignore[arg-type]
        return response

    if payload is None:
        return inner_response or Response(status_code=204)

    response_body = payload
    if _is_cookie(access_transport) and payload.access_token is not None:
        response_body = _strip_for_cookie(response_body, field="access_token")
    if (
        refresh_transport is not None
        and _is_cookie(refresh_transport)
        and payload.refresh_token is not None
    ):
        response_body = _strip_for_cookie(response_body, field="refresh_token")

    response = _build_json_response(response_body)

    if _is_cookie(access_transport) and payload.access_token is not None:
        _set_token_cookie(
            response,
            spec=access_transport,  # type: ignore[arg-type]
            token=payload.access_token,
            expires_in=payload.access_expires_in,
        )
    if (
        refresh_transport is not None
        and _is_cookie(refresh_transport)
        and payload.refresh_token is not None
    ):
        _set_token_cookie(
            response,
            spec=refresh_transport,  # type: ignore[arg-type]
            token=payload.refresh_token,
            expires_in=payload.refresh_expires_in,
        )

    return response
