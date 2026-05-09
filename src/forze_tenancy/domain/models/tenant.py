from pydantic import Field

from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

# ----------------------- #


class TenantImmutableFields(CoreModel):
    """Immutable tenant catalog fields."""

    tenant_key: str | None = Field(default=None, frozen=True)
    """Optional stable external key."""


# ....................... #


class Tenant(Document, TenantImmutableFields):
    """Tenant aggregate root."""

    is_active: bool = True
    """Whether the tenant participates in routing and policy."""


# ....................... #


class CreateTenantCmd(CreateDocumentCmd, TenantImmutableFields):
    """Create tenant command."""


# ....................... #


class UpdateTenantCmd(BaseDTO):
    """Partial update for tenant."""

    is_active: bool | None = None


# ....................... #


class ReadTenant(ReadDocument, TenantImmutableFields):
    """Tenant read model."""

    is_active: bool
