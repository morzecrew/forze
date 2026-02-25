"""Prebuilt routers for common Forze usecases."""

from .document import build_document_router, document_facade_dependency

# ----------------------- #

__all__ = [
    "build_document_router",
    "document_facade_dependency",
]
