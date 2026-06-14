"""Request/response DTOs for the tenant selector (list + switch)."""

from uuid import UUID

from pydantic import Field

from forze.domain.models import BaseDTO

# ----------------------- #


class TenantListItemDTO(BaseDTO):
    """One tenant the principal can act as."""

    tenant_id: UUID
    """Tenant identifier."""

    tenant_key: str | None = None
    """Human-facing tenant key, when known."""

    is_current: bool = False
    """Whether this is the tenant currently bound to the request."""


# ....................... #


class TenantListDTO(BaseDTO):
    """The authenticated principal's active tenant memberships."""

    tenants: list[TenantListItemDTO] = Field(default_factory=list)


# ....................... #


class TenantSwitchRequestDTO(BaseDTO):
    """Request to activate (switch to) one of the principal's tenants."""

    id: UUID
    """Tenant id to activate (must be one of the principal's memberships)."""


# ....................... #


class TenantLeaveRequestDTO(BaseDTO):
    """Request to drop the principal's own membership in a tenant."""

    id: UUID
    """Tenant id to leave (the principal's own membership is removed)."""
