"""Constants for the forze_s3 package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeS3Logger(StrEnum):
    """Forze S3 logger names."""

    KERNEL = "forze_s3.kernel"


# ....................... #

FORZE_S3_LOGGER_NAMES: Final = list(map(str, ForzeS3Logger))
