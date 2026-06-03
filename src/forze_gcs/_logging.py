"""Constants for the forze_gcs package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeGcsLogger(StrEnum):
    """Forze GCS logger names."""

    KERNEL = "forze_gcs.kernel"


# ....................... #

FORZE_GCS_LOGGER_NAMES: Final = list(map(str, ForzeGcsLogger))
