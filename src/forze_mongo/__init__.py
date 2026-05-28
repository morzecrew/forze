"""Mongo integration for Forze."""

from ._compat import require_mongo

require_mongo()

# ....................... #

from .execution import (
    MongoClientDepKey,
    MongoDepsModule,
    MongoDocumentConfig,
    MongoReadOnlyDocumentConfig,
    mongo_document_index_spec_for_binding,
    mongo_document_index_validation_lifecycle_step,
    mongo_lifecycle_step,
    routed_mongo_lifecycle_step,
)
from .kernel.platform import (
    MongoClient,
    MongoClientPort,
    MongoConfig,
    RoutedMongoClient,
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
    "mongo_document_index_spec_for_binding",
    "mongo_document_index_validation_lifecycle_step",
    "MongoDocumentConfig",
    "MongoReadOnlyDocumentConfig",
]
