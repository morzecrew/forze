from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any

from forze.application.composition.authn import AuthnUsecasesFacade
from forze.application.dto import (
    AuthnChangePasswordRequestDTO,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    AuthnTokenResponseDTO,
)
from forze.domain.models import BaseDTO

from .._utils import path_coerce
from ..http import (
    AuthnRequirement,
    BodyAsIsMapper,
    EmptyMapper,
    HttpEndpointSpec,
    HttpMetadataSpec,
    HttpRequestSpec,
    HttpSpec,
    build_http_endpoint_spec,
)
from ..http.policy import apply_authn_requirement
from .features import (
    TokenTransportInputFeature,
    TokenTransportOutputFeature,
)
from .specs import TokenTransportSpec

# ----------------------- #

Facade = AuthnUsecasesFacade

# ....................... #


PasswordLoginEndpointSpec = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    AuthnLoginRequestDTO,
    AuthnLoginRequestDTO,
    AuthnTokenResponseDTO,
    AuthnTokenResponseDTO,
    Facade,
]


def build_authn_password_login_endpoint_spec(
    *,
    access_transport: TokenTransportSpec,
    refresh_transport: TokenTransportSpec | None = None,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> PasswordLoginEndpointSpec:
    """Build the password-login endpoint spec.

    The request body is always read as a HTML form (``application/x-www-form-urlencoded``)
    so first-party login forms can post directly without JSON encoding. The
    response body conforms to :class:`AuthnTokenResponseDTO`; tokens are stripped
    when the matching transport is cookie (the cookie carries the value).
    """

    path = path_coerce(path_override or "/login")

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, AuthnLoginRequestDTO] = {
        "body_type": AuthnLoginRequestDTO,
        "body_mode": "form",
    }

    output_feature: TokenTransportOutputFeature[Any, Any, Any, Any, AuthnLoginRequestDTO, AuthnLoginRequestDTO, AuthnTokenResponseDTO, AuthnTokenResponseDTO, Facade] = TokenTransportOutputFeature(
        access_transport=access_transport,
        refresh_transport=refresh_transport,
        mode="issue",
    )

    return build_http_endpoint_spec(
        Facade,
        Facade.password_login,  # type: ignore[arg-type]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=AuthnTokenResponseDTO,
        mapper=BodyAsIsMapper(AuthnLoginRequestDTO),
        features=[output_feature],
    )


# ....................... #


RefreshEndpointSpec = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    Any,
    AuthnRefreshRequestDTO,
    AuthnTokenResponseDTO,
    AuthnTokenResponseDTO,
    Facade,
]


def build_authn_refresh_endpoint_spec(
    *,
    access_transport: TokenTransportSpec,
    refresh_transport: TokenTransportSpec,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> RefreshEndpointSpec:
    """Build the refresh endpoint spec.

    The refresh token is always read from the configured ``refresh_transport``
    (cookie or header); the endpoint does not declare a request body. The
    response shape mirrors password login.
    """

    path = path_coerce(path_override or "/refresh")

    http_spec: HttpSpec = {"method": "POST", "path": path}

    input_feature: TokenTransportInputFeature[Any, Any, Any, Any, Any, AuthnRefreshRequestDTO, AuthnTokenResponseDTO, AuthnTokenResponseDTO, Facade] = TokenTransportInputFeature(refresh_transport=refresh_transport)
    output_feature: TokenTransportOutputFeature[Any, Any, Any, Any, Any, AuthnRefreshRequestDTO, AuthnTokenResponseDTO, AuthnTokenResponseDTO, Facade] = TokenTransportOutputFeature(
        access_transport=access_transport,
        refresh_transport=refresh_transport,
        mode="issue",
    )

    return build_http_endpoint_spec(
        Facade,
        Facade.refresh_tokens,  # type: ignore[arg-type]
        http=http_spec,
        metadata=metadata,
        response=AuthnTokenResponseDTO,
        mapper=_RefreshTokenPlaceholderMapper(),
        features=[input_feature, output_feature],
    )


# ....................... #


import attrs

from forze.application.contracts.mapping import MapperPort
from forze.application.execution import ExecutionContext

from ..http.contracts import HttpRequestDTO


@attrs.define(slots=True, kw_only=True, frozen=True)
class _RefreshTokenPlaceholderMapper(
    MapperPort[HttpRequestDTO[Any, Any, Any, Any, Any], AuthnRefreshRequestDTO],
):
    """Map an empty request DTO to a placeholder ``AuthnRefreshRequestDTO``.

    The :class:`TokenTransportInputFeature` replaces the placeholder token with
    the value read from the configured transport before the usecase runs.
    """

    async def __call__(
        self,
        dto: HttpRequestDTO[Any, Any, Any, Any, Any],
        *,
        ctx: ExecutionContext | None = None,
    ) -> AuthnRefreshRequestDTO:
        _ = dto, ctx
        return AuthnRefreshRequestDTO(refresh_token="")  # nosec B106


# ....................... #


LogoutEndpointSpec = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    Any,
    BaseDTO,
    None,
    None,
    Facade,
]


def build_authn_logout_endpoint_spec(
    *,
    access_transport: TokenTransportSpec,
    refresh_transport: TokenTransportSpec | None = None,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> LogoutEndpointSpec:
    """Build the logout endpoint spec.

    Always returns ``204`` and clears any cookie-transported tokens. Auth is
    expected to be enforced by an :class:`AuthnRequirement` provided at attach
    time.
    """

    path = path_coerce(path_override or "/logout")

    http_spec: HttpSpec = {"method": "POST", "path": path, "status_code": 204}

    output_feature: TokenTransportOutputFeature[Any, Any, Any, Any, Any, BaseDTO, None, None, Facade] = TokenTransportOutputFeature(
        access_transport=access_transport,
        refresh_transport=refresh_transport,
        mode="clear",
    )

    return build_http_endpoint_spec(
        Facade,
        Facade.logout,  # type: ignore[arg-type]
        http=http_spec,
        metadata=metadata,
        mapper=EmptyMapper(),
        features=[output_feature],
    )


# ....................... #


ChangePasswordEndpointSpec = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    AuthnChangePasswordRequestDTO,
    AuthnChangePasswordRequestDTO,
    None,
    None,
    Facade,
]


def build_authn_change_password_endpoint_spec(
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
    body_mode: str = "form",
) -> ChangePasswordEndpointSpec:
    """Build the change-password endpoint spec.

    Defaults to ``application/x-www-form-urlencoded`` body to match the password
    login endpoint; passing ``body_mode="json"`` switches to JSON. Auth is
    expected to be enforced by an :class:`AuthnRequirement` provided at attach
    time.
    """

    if body_mode not in ("form", "json"):
        raise ValueError("body_mode must be 'form' or 'json'")

    path = path_coerce(path_override or "/change-password")

    http_spec: HttpSpec = {"method": "POST", "path": path, "status_code": 204}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, AuthnChangePasswordRequestDTO] = {
        "body_type": AuthnChangePasswordRequestDTO,
        "body_mode": body_mode,  # type: ignore[typeddict-item]
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.change_password,  # type: ignore[arg-type]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        mapper=BodyAsIsMapper(AuthnChangePasswordRequestDTO),
    )


# ....................... #


__all__ = [
    "build_authn_change_password_endpoint_spec",
    "build_authn_logout_endpoint_spec",
    "build_authn_password_login_endpoint_spec",
    "build_authn_refresh_endpoint_spec",
    "ChangePasswordEndpointSpec",
    "LogoutEndpointSpec",
    "PasswordLoginEndpointSpec",
    "RefreshEndpointSpec",
]


# Keep an explicit re-export of helpers used by attach.py to avoid surprising
# downstream consumers depending on this implementation file directly.
_ = AuthnRequirement, apply_authn_requirement
