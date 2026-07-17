"""Mongo execution configs (frozen attrs)."""

from .counter import MongoCounterConfig
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
    "MongoCounterConfig",
    "MongoDocumentConfig",
    "MongoOutboxConfig",
    "MongoReadOnlyDocumentConfig",
    "MongoSearchConfig",
    "MongoSearchEngine",
    "MongoSearchEngineSpec",
    "MongoTextEngine",
    "MongoVectorEngine",
]
