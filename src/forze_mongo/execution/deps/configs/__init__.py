"""Mongo execution configs (frozen attrs)."""

from .document import MongoDocumentConfig, MongoReadOnlyDocumentConfig
from .outbox import MongoOutboxConfig
from .search import (
    MongoAtlasEngine,
    MongoSearchConfig,
    MongoSearchEngine,
    MongoSearchEngineSpec,
    MongoTextEngine,
    MongoVectorEngine,
)

# ----------------------- #

__all__ = [
    "MongoAtlasEngine",
    "MongoDocumentConfig",
    "MongoOutboxConfig",
    "MongoReadOnlyDocumentConfig",
    "MongoSearchConfig",
    "MongoSearchEngine",
    "MongoSearchEngineSpec",
    "MongoTextEngine",
    "MongoVectorEngine",
]
