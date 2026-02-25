from forze.application.kernel.dependencies import DependencyKey

from ..kernel.platform import MongoClient

# ----------------------- #

MongoClientDependencyKey: DependencyKey[MongoClient] = DependencyKey("mongo_client")
"""Key used to register the :class:`MongoClient` implementation."""
