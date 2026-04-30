"""Mongo integration for Forze."""

from ._compat import require_mongo

require_mongo()

# ....................... #

from .execution import (
    MongoClientDepKey,
    MongoDepsModule,
    MongoDocumentConfig,
    MongoReadOnlyDocumentConfig,
    mongo_lifecycle_step,
    routed_mongo_lifecycle_step,
)
from .kernel.platform import (
    MongoClient,
    MongoClientPort,
    MongoConfig,
    RoutedMongoClient,
    mongo_handled,
)

# ----------------------- #

__all__ = [
    "MongoDepsModule",
    "MongoClient",
    "MongoClientPort",
    "MongoConfig",
    "RoutedMongoClient",
    "MongoClientDepKey",
    "mongo_lifecycle_step",
    "routed_mongo_lifecycle_step",
    "mongo_handled",
    "MongoDocumentConfig",
    "MongoReadOnlyDocumentConfig",
]
