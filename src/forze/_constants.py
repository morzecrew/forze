"""Constants for the forze package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeLogger(StrEnum):
    """Forze logger names."""

    BASE = "forze.base"
    APPLICATION = "forze.application"
    DOMAIN = "forze.domain"
    UNCAUGHT = "forze.uncaught"


# ....................... #

FORZE_LOGGER_NAMES: Final = list(map(str, ForzeLogger))
