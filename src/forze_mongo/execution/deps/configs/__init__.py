"""Mongo execution configs (frozen attrs)."""

from .document import MongoDocumentConfig, MongoReadOnlyDocumentConfig
from .search import MongoSearchConfig, MongoSearchEngine

# ----------------------- #

__all__ = [
    "MongoDocumentConfig",
    "MongoReadOnlyDocumentConfig",
    "MongoSearchConfig",
    "MongoSearchEngine",
]
