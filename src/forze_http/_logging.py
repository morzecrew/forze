"""Constants for the forze_http package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeHttpLogger(StrEnum):
    """Forze HTTP logger names."""

    KERNEL = "http.kernel"
    ADAPTERS = "http.adapters"
    EXECUTION = "http.execution"


# ....................... #

FORZE_HTTP_LOGGER_NAMES: Final = list(map(str, ForzeHttpLogger))
