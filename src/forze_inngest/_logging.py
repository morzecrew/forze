"""Constants for the forze_inngest package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeInngestLogger(StrEnum):
    """Forze Inngest logger names."""

    EXECUTION = "forze_inngest.execution"
    KERNEL = "forze_inngest.kernel"


# ....................... #

FORZE_INNGEST_LOGGER_NAMES: Final = list(map(str, ForzeInngestLogger))
