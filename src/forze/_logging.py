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
