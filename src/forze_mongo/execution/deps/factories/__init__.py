"""Mongo dependency factories."""

from .document import ConfigurableMongoDocument, ConfigurableMongoReadOnlyDocument
from .outbox import (
    ConfigurableMongoOutbox,
    ConfigurableMongoOutboxCommand,
    ConfigurableMongoOutboxQuery,
)
from .search import ConfigurableMongoSearch
from .tx import mongo_txmanager

# ----------------------- #

__all__ = [
    "ConfigurableMongoDocument",
    "ConfigurableMongoOutbox",
    "ConfigurableMongoOutboxCommand",
    "ConfigurableMongoOutboxQuery",
    "ConfigurableMongoReadOnlyDocument",
    "ConfigurableMongoSearch",
    "mongo_txmanager",
]
