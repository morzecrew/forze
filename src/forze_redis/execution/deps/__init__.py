"""Redis dependency keys, module, and factory functions.

Provides :data:`RedisClientDepKey`, :class:`RedisDepsModule`, and factory
functions for counter, document cache, idempotency, pubsub, and stream adapters.
"""

from .keys import RedisClientDepKey
from .module import RedisDepsModule

# ----------------------- #

__all__ = ["RedisDepsModule", "RedisClientDepKey"]
