"""Search operation kernel suffixes for usecase registration and resolution."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class SearchKernelOp(StrEnum):
    """Kernel segments (suffix only) for search usecase operation keys."""

    TYPED = "typed"
    """Search with typed paginated results."""

    RAW = "raw"
    """Search with field-projected raw results."""

    TYPED_CURSOR = "typed_cursor"
    """Search with typed results and cursor-based pagination."""

    RAW_CURSOR = "raw_cursor"
    """Search with field-projected raw results and cursor-based pagination."""
