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
from .kernel.client import (
    MongoClient,
    MongoClientPort,
    MongoConfig,
    RoutedMongoClient,
)
from .kernel.relation import (
    NamedResourceSpec,
    RelationSpec,
    coerce_named_resource_spec,
    coerce_relation_spec,
    is_static_relation,
    relations_match,
    resolve_mongo_collection,
    resolve_mongo_named_resource,
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
    "NamedResourceSpec",
    "RelationSpec",
    "coerce_named_resource_spec",
    "coerce_relation_spec",
    "is_static_relation",
    "relations_match",
    "resolve_mongo_collection",
    "resolve_mongo_named_resource",
]
