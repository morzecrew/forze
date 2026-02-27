"""Redis / Valkey integration for Forze."""

from ._compat import require_redis

require_redis()

# ....................... #

from .dependencies import redis_module
from .kernel.platform import RedisClient, RedisConfig

# ----------------------- #

__all__ = ["redis_module", "RedisClient", "RedisConfig"]
