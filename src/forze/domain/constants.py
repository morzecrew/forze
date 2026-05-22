"""Shared string constants for core domain field names."""

from typing import Final

# ----------------------- #

ID_FIELD: Final = "id"
"""Document identifier field."""

REV_FIELD: Final = "rev"
"""Document revision field."""

LAST_UPDATE_AT_FIELD: Final = "last_update_at"
"""Document last update at field."""

# ....................... #

HISTORY_SOURCE_FIELD: Final = "source"
"""History source field."""

HISTORY_DATA_FIELD: Final = "data"
"""History data field."""

TENANT_ID_FIELD: Final = "tenant_id"  #! Should it be here or in the infra layer ?
"""Tenant identifier field."""
