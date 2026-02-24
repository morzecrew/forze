"""Shared string constants for core domain field names."""

from typing import Final

# ----------------------- #

ID_FIELD: Final[str] = "id"
"""Document identifier field."""

REV_FIELD: Final[str] = "rev"
"""Document revision field."""

SOFT_DELETE_FIELD: Final[str] = "is_deleted"
"""Soft delete field."""

NUMBER_ID_FIELD: Final[str] = "number_id"
"""Number identifier field."""

TENANT_ID_FIELD: Final[str] = "tenant_id"  #! Should it be here or in the infra layer ?
"""Tenant identifier field."""

# ....................... #

HISTORY_SOURCE_FIELD: Final[str] = "source"
"""History source field."""

HISTORY_DATA_FIELD: Final[str] = "data"
"""History data field."""
