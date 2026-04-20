from forze.base.logging import Logger
from forze_redis._logging import ForzeRedisLogger

# ----------------------- #

logger = Logger(str(ForzeRedisLogger.ADAPTERS))
"""Redis adapters logger."""
