"""Group document for membership and group-scoped grants."""

from uuid import UUID

from pydantic import Field

from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

from ..mixins import IsActiveMixin

# ----------------------- #


class GroupImmutableFields(CoreModel):
    """Immutable fields for a group."""

    group_key: str = Field(frozen=True)
    """Stable API-facing group identifier (unique within the catalog scope)."""


# ....................... #


class GroupMutableFields(CoreModel):
    """Mutable fields for a group."""

    description: str | None = None
    """Human-readable description."""

    parent_group_id: UUID | None = None
    """Optional parent group (direct bindings only; transitive closure not applied in v1)."""


# ....................... #


class Group(Document, GroupImmutableFields, GroupMutableFields, IsActiveMixin):
    """Group aggregate for membership and group-level role/permission bindings."""


# ....................... #


class CreateGroupCmd(CreateDocumentCmd, GroupImmutableFields, GroupMutableFields):
    """Create group command."""


# ....................... #


class UpdateGroupCmd(BaseDTO, GroupMutableFields):
    """Update group command."""

    is_active: bool | None = None
    """Whether the group is active."""


# ....................... #


class ReadGroup(ReadDocument, GroupImmutableFields, GroupMutableFields, IsActiveMixin):
    """Read model for group."""
