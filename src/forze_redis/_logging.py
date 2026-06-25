"""Constants for the forze_redis package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeRedisLogger(StrEnum):
    """Forze Redis logger names."""

    ADAPTERS = "forze_redis.adapters"
    EXECUTION = "forze_redis.execution"
    KERNEL = "forze_redis.kernel"


# ....................... #

FORZE_REDIS_LOGGER_NAMES: Final = list(map(str, ForzeRedisLogger))
