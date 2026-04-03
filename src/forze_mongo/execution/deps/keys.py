"""Dependency keys for Mongo-related services."""

from forze.application.contracts.base import DepKey

from ...kernel.platform import MongoClient

# ----------------------- #

MongoClientDepKey: DepKey[MongoClient] = DepKey("mongo_client")
"""Key used to register the :class:`MongoClient` in the deps container."""
