"""Role catalog document."""

from uuid import UUID

from pydantic import Field

from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

# ----------------------- #


class RoleDefinitionImmutableFields(CoreModel):
    """Immutable fields for a role definition."""

    role_key: str = Field(frozen=True)
    """Stable API-facing role identifier (unique within the catalog scope)."""


# ....................... #


class RoleDefinitionMutableFields(CoreModel):
    """Mutable fields for a role definition."""

    description: str | None = None
    """Human-readable description."""

    parent_role_id: UUID | None = None
    """Optional parent role for inheritance (permissions include ancestor roles)."""


# ....................... #


class RoleDefinition(
    Document,
    RoleDefinitionImmutableFields,
    RoleDefinitionMutableFields,
):
    """Catalog entry for a role."""


# ....................... #


class CreateRoleDefinitionCmd(
    CreateDocumentCmd,
    RoleDefinitionImmutableFields,
    RoleDefinitionMutableFields,
):
    """Create role definition command."""


# ....................... #


class UpdateRoleDefinitionCmd(BaseDTO, RoleDefinitionMutableFields):
    """Update role definition command."""


# ....................... #


class ReadRoleDefinition(
    ReadDocument,
    RoleDefinitionImmutableFields,
    RoleDefinitionMutableFields,
):
    """Read model for role definition."""
