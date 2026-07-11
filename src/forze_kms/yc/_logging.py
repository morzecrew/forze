"""Constants for the forze_kms.yc package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeKmsYcLogger(StrEnum):
    """Forze Yandex Cloud KMS logger names."""

    KERNEL = "forze_kms.yc.kernel"


# ....................... #

FORZE_KMS_YC_LOGGER_NAMES: Final = list(map(str, ForzeKmsYcLogger))
