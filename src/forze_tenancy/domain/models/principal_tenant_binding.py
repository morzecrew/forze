from uuid import UUID

from pydantic import Field

from forze.domain.models import CoreModel, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class PrincipalTenantBindingImmutableFields(CoreModel):
    """Immutable principal–tenant membership fields."""

    principal_id: UUID = Field(frozen=True)
    """Principal identifier."""

    tenant_id: UUID = Field(frozen=True)
    """Tenant identifier."""


# ....................... #


class PrincipalTenantBinding(Document, PrincipalTenantBindingImmutableFields):
    """Membership edge: principal belongs to tenant."""


# ....................... #


class CreatePrincipalTenantBindingCmd(
    CreateDocumentCmd,
    PrincipalTenantBindingImmutableFields,
):
    """Create principal–tenant binding command."""


# ....................... #


class ReadPrincipalTenantBinding(ReadDocument, PrincipalTenantBindingImmutableFields):
    """Principal–tenant binding read model."""
