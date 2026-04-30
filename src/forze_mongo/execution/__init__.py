"""Mongo execution wiring for the application kernel."""

from .deps import (
    MongoClientDepKey,
    MongoDepsModule,
    MongoDocumentConfig,
    MongoReadOnlyDocumentConfig,
)
from .lifecycle import mongo_lifecycle_step, routed_mongo_lifecycle_step

# ----------------------- #

__all__ = [
    "MongoDepsModule",
    "MongoClientDepKey",
    "mongo_lifecycle_step",
    "routed_mongo_lifecycle_step",
    "MongoDocumentConfig",
    "MongoReadOnlyDocumentConfig",
]
