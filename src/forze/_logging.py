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
    # adapter/port machinery. Keep in sync with ``INTEGRATION_LOGGER_PREFIX`` in
    # ``forze.base.logging.constants``.
    INTEGRATIONS = "forze.integrations"
