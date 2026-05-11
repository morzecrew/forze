"""HTTP features for authn token transport (read incoming, write outgoing)."""

from typing import Any, Literal, final

import attrs
from fastapi import HTTPException, Response
from fastapi.responses import JSONResponse

from forze.application.dto import AuthnRefreshRequestDTO, AuthnTokenResponseDTO

from ..http.contracts import (
    HttpEndpointContext,
    HttpEndpointFeaturePort,
    HttpEndpointHandlerPort,
)
from ..http.contracts.typevars import B, C, F, H, In, P, Q, R, Raw
from .specs import (
    CookieTokenTransportSpec,
    HeaderTokenTransportSpec,
    TokenTransportSpec,
)

# ----------------------- #


def _is_cookie(spec: TokenTransportSpec) -> bool:
    return spec.get("kind") == "cookie"


def _is_header(spec: TokenTransportSpec) -> bool:
    return spec.get("kind") == "header"


# ....................... #


def _set_token_cookie(
    response: Response,
    *,
    spec: CookieTokenTransportSpec,
    token: str,
    expires_in: int | None,
) -> None:
    cookie_name = spec.get("cookie_name")

    if not cookie_name:
        raise HTTPException(
            status_code=500,
            detail="cookie_name is required for cookie transport",
        )

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


# ....................... #


def _delete_token_cookie(
    response: Response,
    *,
    spec: CookieTokenTransportSpec,
) -> None:
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


# ....................... #


def _build_response(payload: AuthnTokenResponseDTO) -> Response:
    body = payload.model_dump(mode="json", exclude_none=True)
    return JSONResponse(content=body)


# ....................... #


def _strip_for_cookie(payload: AuthnTokenResponseDTO, *, field: str) -> AuthnTokenResponseDTO:
    """Return ``payload`` with ``field`` set to ``None`` so the value lives in the cookie only."""

    return payload.model_copy(update={field: None})


# ....................... #


TokenTransportMode = Literal["issue", "clear"]
"""``issue`` mints fresh cookies / leaves header tokens in the body;
``clear`` deletes the configured cookies (used for logout)."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TokenTransportOutputFeature(
    HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R, F],
):
    """Translate :class:`AuthnTokenResponseDTO` into a HTTP response with cookies / body.

    For ``mode="issue"``: when access/refresh transport is cookie, sets the
    matching ``Set-Cookie`` header and strips the corresponding token from the
    response body; when transport is header, leaves the body intact.

    For ``mode="clear"``: builds an empty ``204``-style response and emits
    ``delete_cookie`` for any cookie-transport configured (does not touch the
    request body).
    """

    access_transport: TokenTransportSpec
    refresh_transport: TokenTransportSpec | None = attrs.field(default=None)
    mode: TokenTransportMode = attrs.field(default="issue")

    # ....................... #

    def wrap(
        self,
        handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R, F],
    ) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R, F]:

        access = self.access_transport
        refresh = self.refresh_transport
        mode = self.mode

        async def wrapped(
            ctx: HttpEndpointContext[Q, P, H, C, B, In, Raw, R, F],
        ) -> R | Response:
            inner_result = await handler(ctx)

            if mode == "clear":
                response = (
                    inner_result
                    if isinstance(inner_result, Response)
                    else Response(status_code=204)
                )

                if _is_cookie(access):
                    _delete_token_cookie(response, spec=access)  # type: ignore[arg-type]

                if refresh is not None and _is_cookie(refresh):
                    _delete_token_cookie(response, spec=refresh)  # type: ignore[arg-type]

                return response

            # mode == "issue"
            if isinstance(inner_result, Response):
                # The handler already produced a Response (rare); pass through.
                return inner_result

            if not isinstance(inner_result, AuthnTokenResponseDTO):
                # Nothing to do — the endpoint isn't returning the expected DTO.
                return inner_result

            payload: AuthnTokenResponseDTO = inner_result

            response_body = payload

            if _is_cookie(access) and payload.access_token is not None:
                response_body = _strip_for_cookie(response_body, field="access_token")

            if (
                refresh is not None
                and _is_cookie(refresh)
                and payload.refresh_token is not None
            ):
                response_body = _strip_for_cookie(response_body, field="refresh_token")

            response = _build_response(response_body)

            if _is_cookie(access) and payload.access_token is not None:
                _set_token_cookie(
                    response,
                    spec=access,  # type: ignore[arg-type]
                    token=payload.access_token,
                    expires_in=payload.access_expires_in,
                )

            if (
                refresh is not None
                and _is_cookie(refresh)
                and payload.refresh_token is not None
            ):
                _set_token_cookie(
                    response,
                    spec=refresh,  # type: ignore[arg-type]
                    token=payload.refresh_token,
                    expires_in=payload.refresh_expires_in,
                )

            return response  # type: ignore[return-value]

        return wrapped


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TokenTransportInputFeature(
    HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R, F],
):
    """Read the refresh token from the configured transport and bind it to ``ctx.input``.

    Only used on the refresh endpoint. The handler input is expected to be an
    :class:`AuthnRefreshRequestDTO`; the feature replaces its ``refresh_token``
    field with the value extracted from the matching cookie or header.
    """

    refresh_transport: TokenTransportSpec

    # ....................... #

    def wrap(
        self,
        handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R, F],
    ) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R, F]:

        spec = self.refresh_transport

        async def wrapped(
            ctx: HttpEndpointContext[Q, P, H, C, B, In, Raw, R, F],
        ) -> R | Response:
            token = _extract_token(ctx, spec)

            if not token:
                raise HTTPException(
                    status_code=401,
                    detail="Refresh token required",
                )

            current_input: Any = ctx.input

            if isinstance(current_input, AuthnRefreshRequestDTO):
                new_input: Any = current_input.model_copy(
                    update={"refresh_token": token},
                )
            else:
                # Best-effort fallback: build a fresh DTO.
                new_input = AuthnRefreshRequestDTO(refresh_token=token)

            new_ctx = attrs.evolve(ctx, input=new_input)

            return await handler(new_ctx)

        return wrapped


# ....................... #


def _extract_token(
    ctx: HttpEndpointContext[Any, Any, Any, Any, Any, Any, Any, Any, Any],
    spec: TokenTransportSpec,
) -> str | None:
    spec_dict: dict[str, Any] = dict(spec)

    if _is_cookie(spec):
        cookie_name = spec_dict.get("cookie_name")
        if not cookie_name:
            return None
        raw = ctx.raw_request.cookies.get(str(cookie_name))
        return str(raw).strip() if raw else None

    if _is_header(spec):
        header_name = spec_dict.get("header_name")
        if not header_name:
            return None
        raw = ctx.raw_request.headers.get(str(header_name))
        if not raw:
            return None
        # Accept either ``Bearer <token>`` or just ``<token>``.
        scheme_hint = spec_dict.get("scheme")
        if scheme_hint and raw.lower().startswith(str(scheme_hint).lower() + " "):
            return raw[len(str(scheme_hint)) + 1 :].strip()
        # Generic split on first whitespace, fall back to the raw value.
        parts = raw.strip().split(maxsplit=1)
        return parts[1].strip() if len(parts) == 2 else parts[0].strip()

    return None


# ....................... #


def default_access_transport() -> HeaderTokenTransportSpec:
    """Default access-token transport: ``Authorization: Bearer ...`` header."""

    return HeaderTokenTransportSpec(
        kind="header",
        header_name="Authorization",
        scheme="Bearer",
    )


def default_refresh_transport() -> HeaderTokenTransportSpec:
    """Default refresh-token transport: ``X-Refresh-Token`` header (no scheme prefix)."""

    return HeaderTokenTransportSpec(
        kind="header",
        header_name="X-Refresh-Token",
        scheme="",
    )
