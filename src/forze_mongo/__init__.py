"""Mongo integration for Forze."""

from ._compat import require_mongo

require_mongo()

# ....................... #

from .execution import MongoClientDepKey, MongoDepsModule, mongo_lifecycle_step
from .kernel.platform import MongoClient, MongoConfig

# ----------------------- #

__all__ = [
    "MongoDepsModule",
    "MongoClient",
    "MongoConfig",
    "MongoClientDepKey",
    "mongo_lifecycle_step",
]
