"""Mongo dependency keys, module, and factory functions."""

from .configs import (
    MongoDocumentConfig,
    MongoReadOnlyDocumentConfig,
    MongoSearchConfig,
    MongoSearchEngine,
)
from .factories import (
    ConfigurableMongoDocument,
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
    "MongoReadOnlyDocumentConfig",
    "MongoSearchConfig",
    "MongoSearchEngine",
    "ConfigurableMongoDocument",
    "ConfigurableMongoReadOnlyDocument",
    "ConfigurableMongoSearch",
    "mongo_txmanager",
]
