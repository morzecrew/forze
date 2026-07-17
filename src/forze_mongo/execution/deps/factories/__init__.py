"""Mongo dependency factories."""

from .counter import ConfigurableMongoCounter, ConfigurableMongoCounterAdmin
from .document import ConfigurableMongoDocument, ConfigurableMongoReadOnlyDocument
from .outbox import (
    ConfigurableMongoOutbox,
    ConfigurableMongoOutboxAdmin,
    ConfigurableMongoOutboxCommand,
    ConfigurableMongoOutboxQuery,
)
from .search import ConfigurableMongoSearch
from .tx import mongo_txmanager

# ----------------------- #

__all__ = [
    "ConfigurableMongoCounter",
    "ConfigurableMongoCounterAdmin",
    "ConfigurableMongoDocument",
    "ConfigurableMongoOutbox",
    "ConfigurableMongoOutboxAdmin",
    "ConfigurableMongoOutboxCommand",
    "ConfigurableMongoOutboxQuery",
    "ConfigurableMongoReadOnlyDocument",
    "ConfigurableMongoSearch",
    "mongo_txmanager",
]
