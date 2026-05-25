"""HTTP bindings and registration builders for authn operations."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from fastapi import Depends, Form, HTTPException, Request, Response

from forze.application.composition.authn.facades import AuthnFacade
from forze.application.handlers.authn import (
    AuthnChangePasswordRequestDTO,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    AuthnTokenResponseDTO,
)
from forze.base.primitives import StrKeyNamespace
from forze_fastapi.transport.http.policies import Policy
from forze_fastapi.transport.http.register import RouteRegistration
from forze_fastapi.transport.http.router import HttpMethod
from forze_fastapi.transport.http.wire.authn import (
    TokenTransportSpec,
    apply_token_transport_output,
    extract_refresh_token,
)

# ----------------------- #


@dataclass(frozen=True, slots=True)
class AuthnHttpBinding:
    method: HttpMethod
    default_path: str
    status_code: int | None = None


AUTHN_HTTP_BINDINGS: dict[str, AuthnHttpBinding] = {
    "password_login": AuthnHttpBinding("POST", "/login"),
    "refresh": AuthnHttpBinding("POST", "/refresh"),
    "logout": AuthnHttpBinding("DELETE", "/logout", status_code=204),
    "change_password": AuthnHttpBinding("POST", "/change-password", status_code=204),
}


def build_authn_registration(
    name: str,
    *,
    path: str,
    operation_id: str,
    namespace: StrKeyNamespace,
    facade_dep: Callable[..., AuthnFacade],
    access_transport: TokenTransportSpec,
    refresh_transport: TokenTransportSpec,
    policies: Sequence[Policy],
    include_in_schema: bool,
) -> RouteRegistration | None:
    http = AUTHN_HTTP_BINDINGS.get(name)
    if http is None:
        return None

    endpoint: Callable[..., Awaitable[Any]]

    if name == "password_login":

        async def _login(
            login: str = Form(),
            password: str = Form(),
            authn: AuthnFacade = Depends(facade_dep),
        ) -> Response:
            result = await authn.password_login(
                AuthnLoginRequestDTO(login=login, password=password),
            )
            return apply_token_transport_output(
                result,
                access_transport=access_transport,
                refresh_transport=refresh_transport,
                mode="issue",
            )

        endpoint = _login

    elif name == "refresh":

        async def _refresh(
            request: Request,
            authn: AuthnFacade = Depends(facade_dep),
        ) -> Response:
            token = extract_refresh_token(request, refresh_transport)
            if not token:
                raise HTTPException(status_code=401, detail="Refresh token required")
            result = await authn.refresh_tokens(AuthnRefreshRequestDTO(refresh_token=token))
            return apply_token_transport_output(
                result,
                access_transport=access_transport,
                refresh_transport=refresh_transport,
                mode="issue",
            )

        endpoint = _refresh

    elif name == "logout":

        async def _logout(
            authn: AuthnFacade = Depends(facade_dep),
        ) -> Response:
            await authn.logout(None)
            return apply_token_transport_output(
                None,
                access_transport=access_transport,
                refresh_transport=refresh_transport,
                mode="clear",
            )

        endpoint = _logout

    elif name == "change_password":

        async def _change_password(
            new_password: str = Form(),
            authn: AuthnFacade = Depends(facade_dep),
        ) -> None:
            await authn.change_password(
                AuthnChangePasswordRequestDTO(new_password=new_password),
            )

        endpoint = _change_password

    else:
        return None

    response_model: type[Any] | None = AuthnTokenResponseDTO
    if name in ("logout", "change_password"):
        response_model = None

    _ = namespace
    return RouteRegistration(
        method=http.method,
        path=path,
        operation_id=operation_id,
        endpoint=endpoint,
        response_model=response_model,
        status_code=http.status_code,
        policies=policies,
        include_in_schema=include_in_schema,
    )
