from uuid import UUID

from pydantic import Field

from forze.domain.models import CoreModel

# ----------------------- #


class TenantIdMixin(CoreModel):
    """Mixin adding an optional tenant ID field."""

    tenant_id: UUID | None = Field(default=None, frozen=True)
    """Tenant ID."""


# ....................... #


class IsActiveMixin(CoreModel):
    """Mixin adding ``is_active`` field."""

    is_active: bool = True
    """Whether the model is active."""
