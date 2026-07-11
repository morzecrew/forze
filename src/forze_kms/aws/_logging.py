"""Constants for the forze_kms.aws package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeKmsAwsLogger(StrEnum):
    """Forze AWS KMS logger names."""

    KERNEL = "forze_kms.aws.kernel"


# ....................... #

FORZE_KMS_AWS_LOGGER_NAMES: Final = list(map(str, ForzeKmsAwsLogger))
