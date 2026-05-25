from .authn import attach_authn_routes
from .document import attach_document_routes
from .search import attach_search_routes
from .storage import attach_storage_routes

__all__ = [
    "attach_authn_routes",
    "attach_document_routes",
    "attach_search_routes",
    "attach_storage_routes",
]
