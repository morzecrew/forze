"""Versioned-facts operation kernel suffixes for usecase registration and resolution."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class VersionedKernelOp(StrEnum):
    """Kernel segments (suffix only) for versioned-facts document usecase operation keys."""

    CORRECT = "correct"
    """Supersede the current version of a fact with a corrected one."""

    HISTORY = "history"
    """Every version of a fact, oldest first."""

    AS_OF = "as_of"
    """The version of a fact that was current at an instant."""
