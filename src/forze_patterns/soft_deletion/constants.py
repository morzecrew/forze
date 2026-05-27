"""Shared string constants for core domain field names."""

from typing import Final

from forze.domain.constants import LAST_UPDATE_AT_FIELD

# ----------------------- #

SOFT_DELETE_FIELD: Final = "is_deleted"
"""Soft delete field."""

ALLOWED_SOFT_DELETE_DIFF_KEYS: Final = frozenset(
    {
        SOFT_DELETE_FIELD,
        LAST_UPDATE_AT_FIELD,
    }
)
"""Allowed soft delete diff keys."""
