"""Mongo execution wiring for the application kernel."""

from .deps import (
    MongoClientDepKey,
    MongoDepsModule,
    MongoDocumentConfig,
    MongoReadOnlyDocumentConfig,
)
from .document_indexes import (
    mongo_document_index_spec_for_binding,
    mongo_document_index_validation_lifecycle_step,
)
from .lifecycle import mongo_lifecycle_step, routed_mongo_lifecycle_step

# ----------------------- #

__all__ = [
    "MongoDepsModule",
    "MongoClientDepKey",
    "mongo_lifecycle_step",
    "routed_mongo_lifecycle_step",
    "mongo_document_index_spec_for_binding",
    "mongo_document_index_validation_lifecycle_step",
    "MongoDocumentConfig",
    "MongoReadOnlyDocumentConfig",
]
