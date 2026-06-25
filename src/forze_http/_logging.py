"""Constants for the forze_http package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeHttpLogger(StrEnum):
    """Forze HTTP logger names."""

    KERNEL = "forze_http.kernel"
    ADAPTERS = "forze_http.adapters"
    EXECUTION = "forze_http.execution"


# ....................... #

FORZE_HTTP_LOGGER_NAMES: Final = list(map(str, ForzeHttpLogger))
