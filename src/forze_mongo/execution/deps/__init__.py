"""Mongo dependency keys, module, and factory functions."""

from .configs import (
    MongoAtlasEngine,
    MongoDocumentConfig,
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
    "ConfigurableMongoInbox",
    "ConfigurableMongoOutbox",
    "ConfigurableMongoReadOnlyDocument",
    "ConfigurableMongoSearch",
    "mongo_txmanager",
]
