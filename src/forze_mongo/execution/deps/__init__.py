"""Mongo dependency keys, module, and factory functions."""

from .configs import (
    MongoDocumentConfig,
    MongoOutboxConfig,
    MongoReadOnlyDocumentConfig,
    MongoSearchConfig,
    MongoSearchEngine,
)
from .factories import (
    ConfigurableMongoDocument,
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
    "MongoOutboxConfig",
    "MongoReadOnlyDocumentConfig",
    "MongoSearchConfig",
    "MongoSearchEngine",
    "ConfigurableMongoDocument",
    "ConfigurableMongoOutbox",
    "ConfigurableMongoReadOnlyDocument",
    "ConfigurableMongoSearch",
    "mongo_txmanager",
]
