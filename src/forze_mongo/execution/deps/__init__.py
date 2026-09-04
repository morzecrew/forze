"""Mongo dependency keys, module, and factory functions."""

from .configs import (
    MongoAtlasEngine,
    MongoDocumentConfig,
    MongoIdempotencyConfig,
    MongoInboxConfig,
    MongoOutboxConfig,
    MongoReadOnlyDocumentConfig,
    MongoSearchConfig,
    MongoSearchEngine,
    MongoSearchEngineSpec,
    MongoTextEngine,
    MongoVectorEngine,
)
from .factories import (
    ConfigurableMongoDocument,
    ConfigurableMongoIdempotency,
    ConfigurableMongoInbox,
    ConfigurableMongoOutbox,
    ConfigurableMongoReadOnlyDocument,
    ConfigurableMongoSearch,
    mongo_txmanager,
)
from .keys import MongoClientDepKey
from .module import MongoDepsModule

# ----------------------- #

__all__ = [
    "MongoDepsModule",
    "MongoClientDepKey",
    "MongoDocumentConfig",
    "MongoIdempotencyConfig",
    "MongoInboxConfig",
    "MongoOutboxConfig",
    "MongoReadOnlyDocumentConfig",
    "MongoSearchConfig",
    "MongoSearchEngine",
    "MongoSearchEngineSpec",
    "MongoAtlasEngine",
    "MongoTextEngine",
    "MongoVectorEngine",
    "ConfigurableMongoDocument",
    "ConfigurableMongoIdempotency",
    "ConfigurableMongoInbox",
    "ConfigurableMongoOutbox",
    "ConfigurableMongoReadOnlyDocument",
    "ConfigurableMongoSearch",
    "mongo_txmanager",
]
