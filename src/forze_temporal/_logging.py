"""Constants for the forze_temporal package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeTemporalLogger(StrEnum):
    """Forze Temporal logger names."""

    ADAPTERS = "temporal.adapters"
    EXECUTION = "temporal.execution"
    KERNEL = "temporal.kernel"


# ....................... #

FORZE_TEMPORAL_LOGGER_NAMES: Final = list(map(str, ForzeTemporalLogger))
