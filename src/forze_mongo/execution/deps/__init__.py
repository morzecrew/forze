"""Mongo dependency keys, module, and factory functions."""

from .configs import MongoDocumentConfig, MongoReadOnlyDocumentConfig
from .keys import MongoClientDepKey
from .module import MongoDepsModule

# ----------------------- #

__all__ = [
    "MongoDepsModule",
    "MongoClientDepKey",
    "MongoDocumentConfig",
    "MongoReadOnlyDocumentConfig",
]
