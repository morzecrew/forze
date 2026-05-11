"""Pre-built FastAPI endpoints for the authn flows (login/refresh/logout/change-password).

Pair with :class:`~forze.application.composition.authn.AuthnUsecasesFacade` and
the matching :class:`~forze_authn.AuthnDepsModule` registration to wire a turnkey
authentication surface onto a FastAPI router.
"""

from .attach import attach_authn_endpoints
from .endpoints import (
    build_authn_change_password_endpoint_spec,
    build_authn_logout_endpoint_spec,
    build_authn_password_login_endpoint_spec,
    build_authn_refresh_endpoint_spec,
)
from .features import (
    TokenTransportInputFeature,
    TokenTransportOutputFeature,
    default_access_transport,
    default_refresh_transport,
)
from .specs import (
    AuthnConfigSpec,
    AuthnEndpointsSpec,
    CookieTokenTransportSpec,
    HeaderTokenTransportSpec,
    TokenTransportSpec,
)

# ----------------------- #

__all__ = [
    "AuthnConfigSpec",
    "AuthnEndpointsSpec",
    "CookieTokenTransportSpec",
    "HeaderTokenTransportSpec",
    "TokenTransportInputFeature",
    "TokenTransportOutputFeature",
    "TokenTransportSpec",
    "attach_authn_endpoints",
    "build_authn_change_password_endpoint_spec",
    "build_authn_logout_endpoint_spec",
    "build_authn_password_login_endpoint_spec",
    "build_authn_refresh_endpoint_spec",
    "default_access_transport",
    "default_refresh_transport",
]
