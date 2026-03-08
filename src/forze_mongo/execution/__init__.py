"""Mongo execution wiring for the application kernel.

Provides :class:`MongoDepsModule` (dependency module registering client,
tx manager, and document port), :data:`MongoClientDepKey`, and
:func:`mongo_lifecycle_step` for startup/shutdown of the Mongo client.
"""

from .deps import MongoClientDepKey, MongoDepsModule
from .lifecycle import mongo_lifecycle_step

# ----------------------- #

__all__ = ["MongoDepsModule", "MongoClientDepKey", "mongo_lifecycle_step"]
