from enum import StrEnum
from typing import final

# ----------------------- #


@final
class SearchOperation(StrEnum):
    """Logical operation identifiers for search usecases."""

    TYPED_SEARCH = "search.typed"
    """Search with typed paginated results."""

    RAW_SEARCH = "search.raw"
    """Search with field-projected raw results."""
