"""Dependency keys for Mongo-related services."""

from forze.application.contracts.base import DepKey

from ...kernel.platform import MongoClientPort

# ----------------------- #

MongoClientDepKey: DepKey[MongoClientPort] = DepKey("mongo_client")
"""Key used to register a Mongo client (single-URI or routed) in the deps container."""
