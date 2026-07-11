"""Constants for the forze_kms.gcp package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeKmsGcpLogger(StrEnum):
    """Forze GCP KMS logger names."""

    KERNEL = "forze_kms.gcp.kernel"


# ....................... #

FORZE_KMS_GCP_LOGGER_NAMES: Final = list(map(str, ForzeKmsGcpLogger))
