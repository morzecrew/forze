"""Constants for the forze_redis package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeRedisLogger(StrEnum):
    """Forze Redis logger names."""

    ADAPTERS = "redis.adapters"
    EXECUTION = "redis.execution"
    KERNEL = "redis.kernel"


# ....................... #

FORZE_REDIS_LOGGER_NAMES: Final = list(map(str, ForzeRedisLogger))
