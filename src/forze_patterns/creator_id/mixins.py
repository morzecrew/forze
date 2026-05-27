from uuid import UUID

from pydantic import Field

from forze.domain.models import BaseDTO, CoreModel

# ----------------------- #


class CreatorIdMixin(CoreModel):
    """Mixin that adds an immutable creator ID field to a document model."""

    creator_id: UUID | None = Field(frozen=True, default=None)
    """Optional author of the document."""


# ....................... #


class CreatorIdCreateCmdMixin(BaseDTO):
    """Mixin that adds an optional ``creator_id`` field to a create command DTO."""

    creator_id: UUID | None = None
    """Optional author of the document."""


# ....................... #


class CreatorIdUpdateCmdMixin(CreatorIdCreateCmdMixin):
    """Mixin that adds an optional ``creator_id`` field to an update command DTO."""
