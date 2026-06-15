"""Generated routes projecting registry operations onto FastAPI routers."""

from ._attach import RouteStyle
from .authn import attach_authn_routes
from .health import attach_readiness_route
from .jwks import attach_jwks_route
from .document import attach_document_routes
from .search import attach_search_routes
from .storage import DEFAULT_MAX_UPLOAD_SIZE, attach_storage_routes
from .tenancy import attach_tenancy_routes
from .tenancy_admin import attach_tenancy_admin_routes

# ----------------------- #

__all__ = [
    "DEFAULT_MAX_UPLOAD_SIZE",
    "RouteStyle",
    "attach_authn_routes",
    "attach_readiness_route",
    "attach_jwks_route",
    "attach_document_routes",
    "attach_search_routes",
    "attach_storage_routes",
    "attach_tenancy_routes",
    "attach_tenancy_admin_routes",
]
