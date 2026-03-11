"""Prebuilt routers for common Forze usecases."""

from .document import (
    DocumentETagProvider,
    attach_document_routes,
    build_document_router,
    document_facade_dependency,
)
from .search import attach_search_routes, build_search_router, search_facade_dependency

# ----------------------- #

__all__ = [
    "DocumentETagProvider",
    "build_document_router",
    "document_facade_dependency",
    "attach_document_routes",
    "attach_search_routes",
    "build_search_router",
    "search_facade_dependency",
]
