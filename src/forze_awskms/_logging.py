"""Constants for the forze_awskms package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeAwsKmsLogger(StrEnum):
    """Forze AWS KMS logger names."""

    KERNEL = "forze_awskms.kernel"


# ....................... #

FORZE_AWSKMS_LOGGER_NAMES: Final = list(map(str, ForzeAwsKmsLogger))
