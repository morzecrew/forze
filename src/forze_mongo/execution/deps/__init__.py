"""Mongo dependency keys, module, and factory functions."""

from .keys import MongoClientDepKey
from .module import MongoDepsModule

# ----------------------- #

__all__ = ["MongoDepsModule", "MongoClientDepKey"]
