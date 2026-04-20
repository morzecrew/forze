"""Constants for the forze_fastapi package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeFastAPILogger(StrEnum):
    """Forze FastAPI logger names."""

    ACCESS = "fastapi.access"
    ENDPOINTS = "fastapi.endpoints"
    MIDDLEWARES = "fastapi.middlewares"


# ....................... #

FORZE_FASTAPI_LOGGER_NAMES: Final = list(map(str, ForzeFastAPILogger))
