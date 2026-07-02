"""Constants for the forze package."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class ForzeLogger(StrEnum):
    """Forze logger names."""

    BASE = "forze.base"
    APPLICATION = "forze.application"
    DOMAIN = "forze.domain"
    UNCAUGHT = "forze.uncaught"

    # Parent of the dynamic ``forze.integrations.<domain>`` loggers used by shared
    # adapter/port machinery. Single source of truth for the prefix —
    # ``forze.base.logging.constants.INTEGRATION_LOGGER_PREFIX`` derives from this.
    INTEGRATIONS = "forze.integrations"
