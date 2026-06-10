"""Constants for the forze_fastapi package."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class ForzeFastAPILogger(StrEnum):
    """Forze FastAPI logger names."""

    ACCESS = "fastapi.access"
    ERRORS = "fastapi.errors"
