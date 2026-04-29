from uuid import UUID

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TenantIdentity:
    """Tenant identity representation."""

    tenant_id: UUID
    """Tenant ID."""
