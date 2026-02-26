from forze.application.kernel.deps import DepKey

from ..kernel.platform import MongoClient

# ----------------------- #

MongoClientDepKey: DepKey[MongoClient] = DepKey("mongo_client")
"""Key used to register the :class:`MongoClient` implementation."""
