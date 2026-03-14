"""Prebuilt routers for common Forze usecases."""

from ._utils import facade_dependency
from .document import (
    DocumentETagProvider,
    attach_document_routes,
    build_document_router,
)
from .search import attach_search_routes, build_search_router

# ----------------------- #

__all__ = [
    "DocumentETagProvider",
    "build_document_router",
    "attach_document_routes",
    "attach_search_routes",
    "build_search_router",
    "facade_dependency",
]
