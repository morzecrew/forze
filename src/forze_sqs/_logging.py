"""Constants for the forze_sqs package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeSQSLogger(StrEnum):
    """Forze SQS logger names."""

    ADAPTERS = "forze_sqs.adapters"
    KERNEL = "forze_sqs.kernel"


# ....................... #

FORZE_SQS_LOGGER_NAMES: Final = list(map(str, ForzeSQSLogger))
