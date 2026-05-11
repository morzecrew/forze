from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Callable, Sequence

from fastapi import APIRouter

from forze.application.contracts.authn import AuthnSpec
from forze.application.execution import ExecutionContext, UsecaseRegistry
from forze.base.errors import CoreError

from ..http import (
    AuthnRequirement,
    HttpEndpointSpec,
    SimpleHttpEndpointSpec,
    attach_http_endpoint,
)
from ..http.policy import (
    AnyFeature,
    apply_authn_requirement,
    with_default_http_features,
)
from .endpoints import (
    build_authn_change_password_endpoint_spec,
    build_authn_logout_endpoint_spec,
    build_authn_password_login_endpoint_spec,
    build_authn_refresh_endpoint_spec,
)
from .features import default_access_transport, default_refresh_transport
from .specs import AuthnEndpointsSpec, TokenTransportSpec

# ----------------------- #


HttpEndSpec = HttpEndpointSpec[Any, Any, Any, Any, Any, Any, Any, Any, Any]


def _derive_authn_requirement(
    spec: AuthnSpec,
    access_transport: TokenTransportSpec,
) -> AuthnRequirement:
    """Build the default :class:`AuthnRequirement` from the access transport."""

    transport_dict: dict[str, Any] = dict(access_transport)

    if transport_dict.get("kind") == "cookie":
        cookie_name = transport_dict.get("cookie_name")

        if not cookie_name:
            raise CoreError("cookie_name is required for cookie access transport")

        return AuthnRequirement(authn_route=spec.name, token_cookie=str(cookie_name))

    header_name = transport_dict.get("header_name")

    if not header_name:
        raise CoreError("header_name is required for header access transport")

    return AuthnRequirement(authn_route=spec.name, token_header=str(header_name))


# ....................... #


def attach_authn_endpoints(
    router: APIRouter,
    *,
    spec: AuthnSpec,
    registry: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
    endpoints: AuthnEndpointsSpec | None = None,
    exclude_none: bool = True,
    default_http_features: Sequence[AnyFeature] | None = None,
) -> APIRouter:
    """Attach authn endpoints (login, refresh, logout, change_password).

    Each endpoint is opt-in; pass ``True`` (defaults) or a
    :class:`SimpleHttpEndpointSpec` (path/metadata/authn requirement
    overrides) to enable it. Logout and change-password are auto-protected by
    an :class:`AuthnRequirement` derived from ``access_token_transport`` unless
    the caller already supplied one in ``SimpleHttpEndpointSpec[\"authn\"]``.
    """

    endpoints = endpoints or {}
    config = endpoints.get("config", {})

    access_transport = config.get(
        "access_token_transport",
        default_access_transport(),
    )
    refresh_transport = config.get(
        "refresh_token_transport",
        default_refresh_transport(),
    )

    # ....................... #

    def _normalize(
        entry: SimpleHttpEndpointSpec | bool,
    ) -> SimpleHttpEndpointSpec:
        return entry if isinstance(entry, dict) else SimpleHttpEndpointSpec()

    def _ensure_authn(
        simple: SimpleHttpEndpointSpec,
    ) -> SimpleHttpEndpointSpec:
        """Auto-apply default :class:`AuthnRequirement` when none provided."""

        if simple.get("authn") is not None:
            return simple

        new = dict(simple)
        new["authn"] = _derive_authn_requirement(spec, access_transport)
        return new  # type: ignore[return-value]

    def _apply_defaults(
        endpoint_spec: HttpEndSpec,
        simple: SimpleHttpEndpointSpec,
    ) -> HttpEndSpec:
        with_defaults = with_default_http_features(
            endpoint_spec,
            default_http_features,
        )
        return apply_authn_requirement(with_defaults, simple.get("authn"))

    # ....................... #

    password_login_entry = endpoints.get("password_login", False)

    if password_login_entry is not False:
        simple = _normalize(password_login_entry)

        login_spec: HttpEndSpec = build_authn_password_login_endpoint_spec(
            access_transport=access_transport,
            refresh_transport=refresh_transport,
            path_override=simple.get("path_override"),
            metadata=simple.get("metadata"),
        )

        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(login_spec, simple),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    # ....................... #

    refresh_entry = endpoints.get("refresh", False)

    if refresh_entry is not False:
        simple = _normalize(refresh_entry)

        refresh_spec: HttpEndSpec = build_authn_refresh_endpoint_spec(
            access_transport=access_transport,
            refresh_transport=refresh_transport,
            path_override=simple.get("path_override"),
            metadata=simple.get("metadata"),
        )

        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(refresh_spec, simple),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    # ....................... #

    logout_entry = endpoints.get("logout", False)

    if logout_entry is not False:
        simple = _ensure_authn(_normalize(logout_entry))

        logout_spec: HttpEndSpec = build_authn_logout_endpoint_spec(
            access_transport=access_transport,
            refresh_transport=refresh_transport,
            path_override=simple.get("path_override"),
            metadata=simple.get("metadata"),
        )

        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(logout_spec, simple),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    # ....................... #

    change_password_entry = endpoints.get("change_password", False)

    if change_password_entry is not False:
        simple = _ensure_authn(_normalize(change_password_entry))

        change_password_spec: HttpEndSpec = build_authn_change_password_endpoint_spec(
            path_override=simple.get("path_override"),
            metadata=simple.get("metadata"),
        )

        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(change_password_spec, simple),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    return router
