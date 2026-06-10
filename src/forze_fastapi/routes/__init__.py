"""Generated routes projecting registry operations onto FastAPI routers."""

from ._attach import RouteStyle
from .document import attach_document_routes
from .search import attach_search_routes
from .storage import attach_storage_routes

# ----------------------- #

__all__ = [
    "RouteStyle",
    "attach_document_routes",
    "attach_search_routes",
    "attach_storage_routes",
]
