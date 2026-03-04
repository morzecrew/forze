"""Prebuilt routers for common Forze usecases."""

from .document import build_document_router, document_facade_dependency
from .search import build_search_router, search_facade_dependency

# ----------------------- #

__all__ = [
    "build_document_router",
    "document_facade_dependency",
    "build_search_router",
    "search_facade_dependency",
]
