"""Private tenancy warning descriptors for Mongo deps module."""

from forze.application.contracts.tenancy import IntegrationRouteWarning

from .configs import (
    MongoCounterConfig,
    MongoDocumentConfig,
    MongoOutboxConfig,
    MongoReadOnlyDocumentConfig,
    MongoSearchConfig,
)

# ----------------------- #

MONGO_DOCUMENT_RO_WARNING = IntegrationRouteWarning[MongoReadOnlyDocumentConfig](
    kind="document",
    tenant_aware=lambda config: config.tenant_aware,
    relation_fields=lambda config: [("read", config.read)],
)

MONGO_DOCUMENT_RW_WARNING = IntegrationRouteWarning[MongoDocumentConfig](
    kind="document",
    tenant_aware=lambda config: config.tenant_aware,
    relation_fields=lambda config: [
        ("read", config.read),
        ("write", config.write),
        ("history", config.history),
    ],
)

MONGO_SEARCH_WARNING = IntegrationRouteWarning[MongoSearchConfig](
    kind="search",
    tenant_aware=lambda config: config.tenant_aware,
    relation_fields=lambda config: [("read", config.read)],
    named_fields=lambda config: [("index_name", config.index_name)],
)

MONGO_OUTBOX_WARNING = IntegrationRouteWarning[MongoOutboxConfig](
    kind="outbox",
    tenant_aware=lambda config: config.tenant_aware,
    relation_fields=lambda config: [("collection", config.collection)],
)

MONGO_COUNTER_WARNING = IntegrationRouteWarning[MongoCounterConfig](
    kind="counter",
    tenant_aware=lambda config: config.tenant_aware,
    relation_fields=lambda config: [("collection", config.collection)],
)
