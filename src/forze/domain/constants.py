"""Shared string constants for core domain field names.

Used as keys in serialization, validation, and infrastructure layers to ensure
consistent naming across the application.
"""

from typing import Final

# ----------------------- #
#! Maybe these constants as well ...

ID_FIELD: Final[str] = "id"
"""Document identifier field."""

REV_FIELD: Final[str] = "rev"
"""Document revision field."""

SOFT_DELETE_FIELD: Final[str] = "is_deleted"
"""Soft delete field."""

NUMBER_ID_FIELD: Final[str] = "number_id"
"""Number identifier field."""

# ....................... #
#! Move these constants to the infra layer ! at least not domain definitely

HISTORY_SOURCE_FIELD: Final[str] = "source"
"""History source field."""

HISTORY_DATA_FIELD: Final[str] = "data"
"""History data field."""

TENANT_ID_FIELD: Final[str] = "tenant_id"  #! Should it be here or in the infra layer ?
"""Tenant identifier field."""
