"""Mongo execution configs (frozen attrs)."""

from .document import MongoDocumentConfig, MongoReadOnlyDocumentConfig
from .outbox import MongoOutboxConfig
from .search import MongoSearchConfig, MongoSearchEngine

# ----------------------- #

__all__ = [
    "MongoDocumentConfig",
    "MongoOutboxConfig",
    "MongoReadOnlyDocumentConfig",
    "MongoSearchConfig",
    "MongoSearchEngine",
]
