"""Temporal-validity operation kernel suffixes for usecase registration and resolution."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class TemporalKernelOp(StrEnum):
    """Kernel segments (suffix only) for temporal-validity document usecase operation keys."""

    EFFECTIVE_ON = "effective_on"
    """The row in force for one key on a given day."""

    TIMELINE = "timeline"
    """Every row for one key whose period meets a window, earliest first."""
