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

    TYPED_SEARCH_CURSOR = "search.typed_cursor"
    """Search with typed results and cursor-based pagination."""

    RAW_SEARCH_CURSOR = "search.raw_cursor"
    """Search with field-projected raw results and cursor-based pagination."""
