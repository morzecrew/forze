"""Permission catalog document."""

from pydantic import Field

from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

# ----------------------- #


class PermissionDefinitionImmutableFields(CoreModel):
    """Immutable fields for a permission definition."""

    permission_key: str = Field(frozen=True)
    """Stable API-facing permission identifier (unique within the catalog scope)."""


# ....................... #


class PermissionDefinitionMutableFields(CoreModel):
    """Mutable fields for a permission definition."""

    description: str | None = None
    """Human-readable description."""


# ....................... #


class PermissionDefinition(
    Document,
    PermissionDefinitionImmutableFields,
    PermissionDefinitionMutableFields,
):
    """Catalog entry for a permission."""


# ....................... #


class CreatePermissionDefinitionCmd(
    CreateDocumentCmd,
    PermissionDefinitionImmutableFields,
    PermissionDefinitionMutableFields,
):
    """Create permission definition command."""


# ....................... #


class UpdatePermissionDefinitionCmd(BaseDTO, PermissionDefinitionMutableFields):
    """Update permission definition command."""


# ....................... #


class ReadPermissionDefinition(
    ReadDocument,
    PermissionDefinitionImmutableFields,
    PermissionDefinitionMutableFields,
):
    """Read model for permission definition."""
