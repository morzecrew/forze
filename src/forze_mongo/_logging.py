"""Constants for the forze_mongo package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeMongoLogger(StrEnum):
    """Forze Mongo logger names."""

    ADAPTERS = "forze_mongo.adapters"
    EXECUTION = "forze_mongo.execution"
    KERNEL = "forze_mongo.kernel"


# ....................... #

FORZE_MONGO_LOGGER_NAMES: Final = list(map(str, ForzeMongoLogger))
