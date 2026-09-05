"""Mongo dependency keys, module, and factory functions."""

from .configs import (
    MongoAtlasEngine,
    MongoDocumentConfig,
    MongoDurableRunConfig,
    MongoDurableScheduleConfig,
    MongoDurableStepConfig,
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
    ConfigurableMongoDurableRun,
    ConfigurableMongoDurableSchedule,
    ConfigurableMongoDurableStep,
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
    "MongoDurableRunConfig",
    "MongoDurableScheduleConfig",
    "MongoDurableStepConfig",
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
    "ConfigurableMongoDurableRun",
    "ConfigurableMongoDurableSchedule",
    "ConfigurableMongoDurableStep",
    "ConfigurableMongoIdempotency",
    "ConfigurableMongoInbox",
    "ConfigurableMongoOutbox",
    "ConfigurableMongoReadOnlyDocument",
    "ConfigurableMongoSearch",
    "mongo_txmanager",
]
