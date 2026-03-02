"""Redis dependency keys, module, and factory functions.

Provides :data:`RedisClientDepKey`, :class:`RedisDepsModule`, and factory
functions for counter, document cache, and idempotency adapters.
"""

from .keys import RedisClientDepKey
from .module import RedisDepsModule

# ----------------------- #

__all__ = ["RedisDepsModule", "RedisClientDepKey"]
