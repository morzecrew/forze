"""Mongo dependency factories."""

from .document import ConfigurableMongoDocument, ConfigurableMongoReadOnlyDocument
from .outbox import ConfigurableMongoOutbox
from .search import ConfigurableMongoSearch
from .tx import mongo_txmanager

# ----------------------- #

__all__ = [
    "ConfigurableMongoDocument",
    "ConfigurableMongoOutbox",
    "ConfigurableMongoReadOnlyDocument",
    "ConfigurableMongoSearch",
    "mongo_txmanager",
]
